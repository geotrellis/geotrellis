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

package geotrellis.raster.equalization

import geotrellis.raster.histogram._
import geotrellis.raster.Tile
import geotrellis.util.MethodExtensions


trait SinglebandEqualizationMethods extends MethodExtensions[Tile] {

  /**
    * Given a [[geotrellis.raster.histogram.Histogram]] which
    * summarizes this [[Tile]], equalize the histogram of this tile.
    *
    * @param  histogram  The histogram of this tile
    */
  def equalize[T <: AnyVal](histogram: Histogram[T]): Tile = HistogramEqualization(self, histogram)

  /**
    * Equalize the histogram of this [[Tile]].
    */
  def equalize(): Tile = HistogramEqualization(self)
}
