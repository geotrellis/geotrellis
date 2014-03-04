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

package geotrellis.raster.op.local

import geotrellis._

/**
 * Operation to get the arc sine of values.
 * Always return a double raster.
 * if abs(cell_value) > 1, return NaN in that cell.
 */
object Asin extends Serializable {
  /** Takes the arc sine of each raster cell value. */
  def apply(r:Op[Raster]) =
    r.map(_.convert(TypeDouble).mapDouble (z => math.asin(z))) 
     .withName("Asin")
}
