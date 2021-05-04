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

package geotrellis.spark.store.s3

import geotrellis.layer.SpatialKey
import geotrellis.store.LayerId
import geotrellis.store.s3._
import geotrellis.store.util.BlockingThreadPool

import software.amazon.awssdk.services.s3.model.{PutObjectRequest, PutObjectResponse, S3Exception}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.core.sync.RequestBody
import org.apache.spark.rdd.RDD
import cats.effect.IO
import cats.syntax.apply._
import cats.syntax.either._

import java.net.URI
import scala.concurrent.ExecutionContext

object SaveToS3 {
  /**
    * @param id           A Layer ID
    * @param pathTemplate The template used to convert a Layer ID and a SpatialKey into an S3 URI
    * @return             A functon which takes a spatial key and returns an S3 URI
    */
  def spatialKeyToPath(id: LayerId, pathTemplate: String): SpatialKey => String = {
    // Return λ
    { key =>
      pathTemplate
        .replace("{x}", key.col.toString)
        .replace("{y}", key.row.toString)
        .replace("{z}", id.zoom.toString)
        .replace("{name}", id.name)
    }
  }

  /**
    * @param keyToUri  A function that maps each key to full s3 uri
    * @param rdd       An RDD of K, Byte-Array pairs (where the byte-arrays contains image data) to send to S3
    * @param putObjectModifier  Function that will be applied ot S3 PutObjectRequests, so that they can be modified (e.g. to change the ACL settings)
    * @param s3Client   A function which returns an S3 Client (real or mock) into-which to save the data
    * @param executionContext   A function to get execution context
    */
  def apply[K, V](
    rdd: RDD[(K, V)],
    keyToUri: K => String,
    putObjectModifier: PutObjectRequest => PutObjectRequest = { p => p },
    s3Client: => S3Client = S3ClientProducer.get(),
    executionContext: => ExecutionContext = BlockingThreadPool.executionContext
  )(implicit ev: V => Array[Byte]): Unit = {
    val keyToPrefix: K => (String, String) = key => {
      val uri = new URI(keyToUri(key))
      require(uri.getScheme == "s3", s"SaveToS3 only supports s3 scheme: $uri")
      val bucket = uri.getAuthority
      val prefix = uri.getPath.substring(1) // drop the leading / from the prefix
      (bucket, prefix)
    }

    rdd.foreachPartition { partition =>
      val s3client = s3Client
      val requests: fs2.Stream[IO, (PutObjectRequest, RequestBody)] =
        fs2.Stream.fromIterator[IO](
          partition.map { case (key, data) =>
            val bytes = ev(data)
            val (bucket, path) = keyToPrefix(key)
            val request = PutObjectRequest.builder()
              .bucket(bucket)
              .key(path)
              .contentLength(bytes.length.toLong)
              .build()
            val requestBody = RequestBody.fromBytes(bytes)

            (putObjectModifier(request), requestBody)
          }, 1
        )

      implicit val ec    = executionContext
      
      // TODO: runime should be configured
      import cats.effect.unsafe.implicits.global

      import geotrellis.store.util.IOUtils._
      val write: (PutObjectRequest, RequestBody) => fs2.Stream[IO, PutObjectResponse] =
        (request, requestBody) => {
          fs2.Stream eval IO {
            //request.getInputStream.reset() // reset in case of retransmission to avoid 400 error
            s3client.putObject(request, requestBody)
          }.retryEBO {
            case e: S3Exception if e.statusCode == 503 => true
            case _ => false
          }
        }

    requests
      .map(Function.tupled(write))
      .parJoinUnbounded
      .compile
      .toVector
      .attempt
      .unsafeRunSync()
      .valueOr(throw _)
    }
  }
}
