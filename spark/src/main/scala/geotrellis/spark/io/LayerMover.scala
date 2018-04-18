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

package geotrellis.spark.io

import geotrellis.spark._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.json._
import geotrellis.spark.tiling._
import geotrellis.util._
import geotrellis.vector._

import spray.json._

import scala.reflect.ClassTag

trait LayerMover[ID] {
  def move[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag: SpatialComponent,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: Component[?, Bounds[K]]: Component[?, LayoutDefinition]: Component[?, Extent]
  ](from: ID, to: ID): Unit
}
