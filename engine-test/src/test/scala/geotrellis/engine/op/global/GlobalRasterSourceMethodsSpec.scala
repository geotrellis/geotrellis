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

package geotrellis.engine.op.global

import geotrellis.engine._
import geotrellis.raster.op.global._
import geotrellis.process._

import org.scalatest._

class GlobalRasterSourceMethodsSpec extends FunSpec with TestEngine {

  describe("finding min and max") {
    it("Gets correct min and max for 3 x 2 tiles") {
      val rs1 = createRasterSource(
        Array( 1,1,1,      2,2,2,      3,3,3,
               1,1,1,      2,2,2,      3,3,3,

               4,4,4,      5,5,nd,      6,6,nd,
               4,4,4,      5,5,5,      6,6,6,

               7,7,7,      8,4,8,      9,9,9,
               7,1,7,      8,nd,8,      9,9,9,

              10,10,10,   11,11,11,   12,12,12,
              10,9,10,   11,20,11,   12,12,12
        ),
        3,4,3,2
      )
      rs1.min.get should be (1)
      rs1.max.get should be (20)
      rs1.minMax.get should be ((1,20))
    }
  }

  describe("verticalFlip") {
    it("flips a tiled rastersource the same as one raster") {
      val rs = createRasterSource(
        Array( 1,1,1,      2,2,2,      3,3,3,
          1,1,1,      2,2,2,      3,3,3,

          4,4,4,      5,5,nd,      6,6,nd,
          4,4,4,      5,5,5,      6,6,6,

          7,7,7,      8,4,8,      9,9,9,
          7,1,7,      8,nd,8,      9,9,9,

          10,10,10,   11,11,11,   12,12,12,
          10,9,10,   11,20,11,   12,12,12
        ),
        3,4,3,2
      )
      val t = rs.get

      assertEqual(rs.verticalFlip.get, t.verticalFlip)
    }
  }
}
