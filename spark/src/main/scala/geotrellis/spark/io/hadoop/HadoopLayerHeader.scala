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

package geotrellis.spark.io.hadoop

import geotrellis.spark.io.LayerHeader

import org.apache.hadoop.fs.Path
import spray.json._

case class HadoopLayerHeader(
  keyClass: String,
  valueClass: String,
  path: Path
) extends LayerHeader {
  def format = "hdfs"
}

object HadoopLayerHeader {
  implicit object HadoopLayerMetadataFormat extends RootJsonFormat[HadoopLayerHeader] {
    def write(md: HadoopLayerHeader) =
      JsObject(
        "format" -> JsString(md.format),
        "keyClass" -> JsString(md.keyClass),
        "valueClass" -> JsString(md.valueClass),
        "path" -> JsString(md.path.toString)
      )

    def read(value: JsValue): HadoopLayerHeader =
      value.asJsObject.getFields("keyClass", "valueClass", "path") match {
        case Seq(JsString(keyClass), JsString(valueClass), JsString(path)) =>
          HadoopLayerHeader(
            keyClass, 
            valueClass,
            new Path(path))
        case _ =>
          throw new DeserializationException(s"HadoopLayerMetadata expected, got: $value")
      }
  }
}
