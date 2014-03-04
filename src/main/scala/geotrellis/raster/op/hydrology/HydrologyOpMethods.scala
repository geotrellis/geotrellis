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

package geotrellis.raster.op.hydrology

import geotrellis._
import geotrellis.raster._
import geotrellis.source._
import geotrellis.raster.op.focal.Square

trait HydrologyOpMethods[+Repr <: RasterSource] { self: Repr =>
  def accumulation() = globalOp(Accumulation(_))
  def fill(options: FillOptions) = focal(Square(1)) { (r,n,t) => Fill(r, options, t) }
  def flowDirection() = globalOp(FlowDirection(_))
}
