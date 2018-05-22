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

import geotrellis.spark.io._

import io.circe._
import io.circe.generic.semiauto._

import java.net.URI

case class HadoopLayerHeader(
  keyClass: String,
  valueClass: String,
  path: URI,
  layerType: LayerType = AvroLayerType
) extends LayerHeader {
  def format = "hdfs"
}

object HadoopLayerHeader {
  implicit val hadoopLayerHeaderEncoder: Encoder[HadoopLayerHeader] = deriveEncoder
  implicit val hadoopLayerHeaderDecoder: Decoder[HadoopLayerHeader] =
    Decoder.decodeHCursor.emap { c =>
      (c.downField("keyClass").as[String],
        c.downField("valueClass").as[String],
        c.downField("path").as[URI],
        c.downField("layerType").as[LayerType]) match {
        case (Right(f), Right(kc), Right(p), Right(lt)) => Right(HadoopLayerHeader(f, kc, p, lt))
        case (Right(f), Right(kc), Right(p), _) => Right(HadoopLayerHeader(f, kc, p, AvroLayerType))
        case _ => Left(s"HadoopLayerHeader expected, got: ${c.focus}")
      }
    }
}
