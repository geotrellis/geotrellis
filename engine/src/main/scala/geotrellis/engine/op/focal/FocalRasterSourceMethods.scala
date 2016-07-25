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

package geotrellis.engine.op.focal

import geotrellis.engine._
import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal._

@deprecated("geotrellis-engine has been deprecated", "Geotrellis Version 0.10")
trait FocalRasterSourceMethods extends RasterSourceMethods with FocalOperation {
  def focalSum(n: Neighborhood) = focal(n)(Sum(_, _, _))
  def focalMin(n: Neighborhood) = focal(n)(Min(_, _, _))
  def focalMax(n: Neighborhood) = focal(n)(Max(_, _, _))
  def focalMean(n: Neighborhood) = focal(n)(Mean(_, _, _))
  def focalMedian(n: Neighborhood) = focal(n)(Median(_, _, _))
  def focalMode(n: Neighborhood) = focal(n)(Mode(_, _, _))
  def focalStandardDeviation(n: Neighborhood) = focal(n)(StandardDeviation(_, _, _))
  def focalConway() = focal(Square(1))(Conway.apply)

  def tileMoransI(n: Neighborhood) =
    rasterSource.globalOp(TileMoransICalculation.apply(_, n, None))

  def scalarMoransI(n: Neighborhood): ValueSource[Double] = {
    rasterSource.converge.map(ScalarMoransICalculation(_, n, None))
  }
}
