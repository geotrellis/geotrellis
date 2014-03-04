/*******************************************************************************
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
 ******************************************************************************/

package geotrellis.raster.op.zonal

import geotrellis._
import geotrellis.raster._
import geotrellis.source._

import scalaxy.loops._

import scala.collection.mutable

trait ZonalOpMethods[+Repr <: RasterSource] { self:Repr =>
  /**
   * Given a raster, return a histogram summary of the cells within each zone.
   *
   * @note    zonalHistorgram does not currently support Double raster data.
   *          If you use a Raster with a Double RasterType (TypeFloat,TypeDouble)
   *          the data values will be rounded to integers.
   */
  def zonalHistogram(zonesSource:RasterSource) =
    self.converge.mapOp(ZonalHistogram(_,zonesSource.get))

  /**
   * Given a raster and a raster representing it's zones, sets all pixels
   * within each zone to the percentage of those pixels having values equal 
   * to that of the given pixel.
   *
   * Percentages are integer values from 0 - 100.
   * 
   * @note    ZonalPercentage does not currently support Double raster data.
   *          If you use a Raster with a Double RasterType (TypeFloat,TypeDouble)
   *          the data values will be rounded to integers.
   */
  def zonalPercentage(zonesSource:RasterSource) =
    self.converge.mapOp(ZonalPercentage(_,zonesSource.get))
}
