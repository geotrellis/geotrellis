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

package geotrellis.store.accumulo

import geotrellis.layer._
import geotrellis.store.avro.codecs.KeyValueRecordCodec
import geotrellis.store.avro.{AvroEncoder, AvroRecordCodec}
import geotrellis.store.util.BlockingThreadPool
import org.apache.accumulo.core.data.{Range => AccumuloRange}
import org.apache.accumulo.core.security.Authorizations
import org.apache.avro.Schema
import org.apache.hadoop.io.Text

import cats.effect._
import cats.syntax.apply._
import cats.syntax.either._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

object AccumuloCollectionReader {
  def read[K: Boundable: AvroRecordCodec: ClassTag, V: AvroRecordCodec: ClassTag](
    table: String,
    columnFamily: Text,
    queryKeyBounds: Seq[KeyBounds[K]],
    decomposeBounds: KeyBounds[K] => Seq[AccumuloRange],
    filterIndexOnly: Boolean,
    writerSchema: Option[Schema] = None,
    executionContext: => ExecutionContext = BlockingThreadPool.executionContext
  )(implicit instance: AccumuloInstance): Seq[(K, V)] = {
    if(queryKeyBounds.isEmpty) return Seq.empty[(K, V)]

    val codec = KeyValueRecordCodec[K, V]
    val includeKey = (key: K) => queryKeyBounds.includeKey(key)

    val ranges = queryKeyBounds.flatMap(decomposeBounds).iterator

    implicit val ec = executionContext
    // TODO: runime should be configured
    import cats.effect.unsafe.implicits.global

    val range: fs2.Stream[IO, AccumuloRange] = fs2.Stream.fromIterator[IO](ranges, 1)

    val read = { range: AccumuloRange => fs2.Stream eval IO {
      val scanner = instance.connector.createScanner(table, new Authorizations())
      scanner.setRange(range)
      scanner.fetchColumnFamily(columnFamily)
      val result =
        scanner
          .iterator.asScala
          .map({ entry => AvroEncoder.fromBinary(writerSchema.getOrElse(codec.schema), entry.getValue.get)(codec) })
          .flatMap({ pairs: Vector[(K, V)] =>
            if (filterIndexOnly) pairs
            else pairs.filter { pair => includeKey(pair._1) }
          }).toVector
        scanner.close()
        result
      }
    }

    range
      .map(read)
      .parJoinUnbounded
      .compile
      .toVector
      .map(_.flatten)
      .attempt
      .unsafeRunSync()
      .valueOr(throw _)
  }
}
