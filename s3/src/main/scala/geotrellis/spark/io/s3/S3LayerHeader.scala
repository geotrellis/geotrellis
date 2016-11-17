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

import geotrellis.raster.Tile
import geotrellis.spark.io.LayerHeader

import spray.json._

case class S3LayerHeader(
  keyClass: String,
  valueClass: String,
  bucket: String,
  key: String
) extends LayerHeader {
  def format = "s3"
}

object S3LayerHeader {
  implicit object S3LayerHeaderFormat extends RootJsonFormat[S3LayerHeader] {
    def write(md: S3LayerHeader) =
      JsObject(
        "format" -> JsString(md.format),
        "keyClass" -> JsString(md.keyClass),
        "valueClass" -> JsString(md.valueClass),
        "bucket" -> JsString(md.bucket.toString),
        "key" -> JsString(md.key.toString)
      )

    def read(value: JsValue): S3LayerHeader =
      value.asJsObject.getFields("keyClass", "valueClass", "bucket", "key") match {
        case Seq(JsString(keyClass), JsString(valueClass), JsString(bucket), JsString(key)) =>
          S3LayerHeader(
            keyClass,
            valueClass,
            bucket, key)
        case Seq(JsString(keyClass), JsString(bucket), JsString(key)) =>
          S3LayerHeader(
            keyClass,
            classOf[Tile].getCanonicalName,
            bucket, key)

        case other =>
          throw new DeserializationException(s"S3LayerHeader expected, got: $other")
      }
  }
}
