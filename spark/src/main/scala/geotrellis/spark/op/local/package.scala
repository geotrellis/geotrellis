/*
 * Copyright (c) 2014 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.op

import geotrellis.spark._
import scala.reflect._
import geotrellis.raster.Tile

package object local {
  implicit class LocalRasterRDDMethodExtensions[K](val rasterRDD: RasterRDD[K, Tile])(implicit val keyClassTag: ClassTag[K])
      extends LocalRasterRDDMethods[K] { }

  implicit class LocalRasterRDDSeqExtensions[K](val rasterRDDs: Traversable[RasterRDD[K, Tile]])(implicit val keyClassTag: ClassTag[K])
      extends LocalRasterRDDSeqMethods[K] { }
}
