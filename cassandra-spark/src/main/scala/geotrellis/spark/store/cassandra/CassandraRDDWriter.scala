/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.store.cassandra

import geotrellis.store._
import geotrellis.store.avro._
import geotrellis.store.avro.codecs._
import geotrellis.store.cassandra._
import geotrellis.spark.store._
import geotrellis.spark.util.KryoWrapper
import geotrellis.store.util.BlockingThreadPool

import com.datastax.driver.core.DataType._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.querybuilder.QueryBuilder.{eq => eqs}
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.schemabuilder.SchemaBuilder
import cats.effect.IO
import cats.syntax.apply._
import cats.syntax.either._
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD

import java.nio.ByteBuffer
import java.math.BigInteger

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

object CassandraRDDWriter {
  def write[K: AvroRecordCodec, V: AvroRecordCodec](
    rdd: RDD[(K, V)],
    instance: CassandraInstance,
    layerId: LayerId,
    decomposeKey: K => BigInt,
    keyspace: String,
    table: String,
    executionContext: => ExecutionContext = BlockingThreadPool.executionContext
  ): Unit = update(rdd, instance, layerId, decomposeKey, keyspace, table, None, None, executionContext)

  private[cassandra] def update[K: AvroRecordCodec, V: AvroRecordCodec](
    raster: RDD[(K, V)],
    instance: CassandraInstance,
    layerId: LayerId,
    decomposeKey: K => BigInt,
    keyspace: String,
    table: String,
    writerSchema: Option[Schema],
    mergeFunc: Option[(V,V) => V],
    executionContext: => ExecutionContext = BlockingThreadPool.executionContext
  ): Unit = {
    implicit val sc = raster.sparkContext

    val codec = KeyValueRecordCodec[K, V]

    instance.withSessionDo { session =>
      instance.ensureKeyspaceExists(keyspace, session)
      session.execute(
        SchemaBuilder.createTable(keyspace, table).ifNotExists()
          .addPartitionKey("key", varint)
          .addClusteringColumn("name", text)
          .addClusteringColumn("zoom", cint)
          .addColumn("value", blob)
      )
    }

    val readQuery =
      QueryBuilder.select("value")
        .from(keyspace, table)
        .where(eqs("key", QueryBuilder.bindMarker()))
        .and(eqs("name", layerId.name))
        .and(eqs("zoom", layerId.zoom))
        .toString

    val writeQuery =
      QueryBuilder
        .insertInto(keyspace, table)
        .value("name", layerId.name)
        .value("zoom", layerId.zoom)
        .value("key", QueryBuilder.bindMarker())
        .value("value", QueryBuilder.bindMarker())
        .toString

    val _recordCodec = KeyValueRecordCodec[K, V]
    val kwWriterSchema = KryoWrapper(writerSchema)

    // Call groupBy with numPartitions; if called without that argument or a partitioner,
    // groupBy will reuse the partitioner on the parent RDD if it is set, which could be typed
    // on a key type that may no longer by valid for the key type of the resulting RDD.
      raster.groupBy({ row => decomposeKey(row._1) }, numPartitions = raster.partitions.length)
        .foreachPartition { partition: Iterator[(BigInt, Iterable[(K, V)])] =>
          if(partition.nonEmpty) {
            instance.withSession { session =>
              val readStatement = session.prepare(readQuery)
              val writeStatement = session.prepare(writeQuery)

              val rows: fs2.Stream[IO, (BigInt, Vector[(K,V)])] =
                fs2.Stream.fromIterator[IO](
                  partition.map { case (key, value) => (key, value.toVector) }, 1
                )

              implicit val ec = executionContext
              // TODO: runime should be configured
              import cats.effect.unsafe.implicits.global

              def elaborateRow(row: (BigInt, Vector[(K,V)])): fs2.Stream[IO, (BigInt, Vector[(K,V)])] = {
                fs2.Stream eval IO {
                  val (key, current) = row
                  val updated = LayerWriter.updateRecords(mergeFunc, current, existing = {
                    val oldRow = session.execute(readStatement.bind(key: BigInteger))
                    if (oldRow.asScala.nonEmpty) {
                      val bytes = oldRow.one().getBytes("value").array()
                      val schema = kwWriterSchema.value.getOrElse(_recordCodec.schema)
                      AvroEncoder.fromBinary(schema, bytes)(_recordCodec)
                    } else Vector.empty
                  })

                  (key, updated)
                }
              }

              def rowToBytes(row: (BigInt, Vector[(K,V)])): fs2.Stream[IO, (BigInt, ByteBuffer)] = {
                fs2.Stream eval IO {
                  val (key, kvs) = row
                  val bytes = ByteBuffer.wrap(AvroEncoder.toBinary(kvs)(codec))
                  (key, bytes)
                }
              }

              def retire(row: (BigInt, ByteBuffer)): fs2.Stream[IO, ResultSet] = {
                val (id, value) = row
                fs2.Stream eval IO {
                  session.execute(writeStatement.bind(id: BigInteger, value))
                }
              }

              val results = rows
                .flatMap(elaborateRow)
                .flatMap(rowToBytes)
                .map(retire)
                .parJoinUnbounded
                .onComplete {
                  fs2.Stream eval IO {
                    session.closeAsync()
                    session.getCluster.closeAsync()
                  }
                }

              results
                .compile
                .drain
                .attempt
                .unsafeRunSync()
                .valueOr(throw _)
            }
          }
        }
  }
}
