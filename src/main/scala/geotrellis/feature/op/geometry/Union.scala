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

package geotrellis.feature.op.geometry

import geotrellis._
import geotrellis.feature._
import com.vividsolutions.jts.geom

/** Returns the union of these geometries.
  *  @param g      first geometry to union, whose data is preserved
  *  @param other  other geometry to union
  * 
  *  @tparam A    type of feature data
  *
  *  @see [[http://tsusiatsoftware.net/jts/javadoc/com/vividsolutions/jts/geom/Geometry.html#union(com.vividsolutions.jts.geom.Geometry) "JTS documentation"]]
  */
case class Union[A](g:Op[Geometry[A]], other:Op[Geometry[_]]) extends Op2(g,other) ({
  (g,other) => Result(Feature(g.geom.union(other.geom),g.data))
})
