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

package geotrellis.spark.pointcloud

import java.io.File

import spray.json._
import spray.json.DefaultJsonProtocol._

package object json extends MetadataFormat {
  def getPipelineJson(localPath: File, targetEPSGName: Option[String] = None): JsObject = {
    targetEPSGName match {
      case Some(crs) =>
        JsObject(
          "pipeline" -> JsArray(
            JsObject(
              "filename" -> localPath.getAbsolutePath.toJson
            ),
            JsObject(
              "type" -> "filters.reprojection".toJson,
              "out_srs" -> crs.toJson
            )
          )
        )

      case _ => JsObject(
        "pipeline" -> JsArray(
          JsObject(
            "filename" -> localPath.getAbsolutePath.toJson
          )
        )
      )
    }
  }
}
