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

package geotrellis.spark.io.s3

import geotrellis.tiling.{KeyBounds, Boundable}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.s3.conf.S3Config
import geotrellis.layers.io.avro.codecs.KeyValueRecordCodec
import geotrellis.layers.io.index.{IndexRanges, MergeQueue}
import geotrellis.layers.io.avro.{AvroEncoder, AvroRecordCodec}
import geotrellis.spark.util.KryoWrapper

import software.amazon.awssdk.services.s3.model.{S3Exception, GetObjectRequest}
import software.amazon.awssdk.services.s3.S3Client

import org.apache.avro.Schema
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class S3RDDReader(
  val getClient: () => S3Client = S3ClientProducer.get,
  val defaultThreadCount: Int = S3Config.threads.rdd.readThreads
) {

  def read[
    K: AvroRecordCodec: Boundable,
    V: AvroRecordCodec
  ](
    bucket: String,
    keyPath: BigInt => String,
    queryKeyBounds: Seq[KeyBounds[K]],
    decomposeBounds: KeyBounds[K] => Seq[(BigInt, BigInt)],
    filterIndexOnly: Boolean,
    writerSchema: Option[Schema] = None,
    numPartitions: Option[Int] = None,
    threads: Int = defaultThreadCount
  )(implicit sc: SparkContext): RDD[(K, V)] = {
    if (queryKeyBounds.isEmpty) return sc.emptyRDD[(K, V)]

    val ranges = if (queryKeyBounds.length > 1)
      MergeQueue(queryKeyBounds.flatMap(decomposeBounds))
    else
      queryKeyBounds.flatMap(decomposeBounds)

    val bins = IndexRanges.bin(ranges, numPartitions.getOrElse(sc.defaultParallelism))

    val includeKey = (key: K) => queryKeyBounds.includeKey(key)
    val _recordCodec = KeyValueRecordCodec[K, V]
    val _getS3Client = getClient
    val kwWriterSchema = KryoWrapper(writerSchema) //Avro Schema is not Serializable

    sc.parallelize(bins, bins.size)
      .mapPartitions { partition: Iterator[Seq[(BigInt, BigInt)]] =>
        val s3Client = _getS3Client()
        val writerSchema = kwWriterSchema.value.getOrElse(_recordCodec.schema)
        partition flatMap { seq =>
          LayerReader.njoinEBO[K, V](seq.toIterator, threads)({ index: BigInt =>
            try {
              val request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(keyPath(index))
                .build()
              val is = s3Client.getObject(request)
              val bytes = IOUtils.toByteArray(is)
              is.close()
              val recs = AvroEncoder.fromBinary(writerSchema, bytes)(_recordCodec)
              if (filterIndexOnly) recs
              else recs.filter { row => includeKey(row._1) }
            } catch {
              case e: S3Exception if e.statusCode == 404 => Vector.empty
            }
          })({
            case e: S3Exception if e.statusCode == 500 => true
            case e: S3Exception if e.statusCode == 503 => true
            case _ => false
          })
        }
      }
  }
}

