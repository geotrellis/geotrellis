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

package geotrellis.engine.op.local

import geotrellis.raster._
import geotrellis.engine._

import org.scalatest._


class PowSpec extends FunSpec with Matchers with TestEngine {
  describe("Pow") {
    it("pow's an integer to a raster source") {
      val rs = RasterSource("quad_tiled")
      val r = get(rs)
      run(-10 **: rs) match {
        case Complete(result, success) =>
          for (row <- 0 until r.rows) {
            for (col <- 0 until r.cols) {
              result.get(col, row) should be (math.pow(-10, r.get(col,row)))
            }
          }
        case Error(msg, failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }

    it("pow's two tiled RasterSources correctly") {
      val rs1 = RasterSource("quad_tiled")
      val rs2 = RasterSource("quad_tiled2")

      val r1 = get(rs1)
      val r2 = get(rs2)
      run(rs1 ** rs2) match {
        case Complete(result,success) =>
          //println(success)
          for(row <- 0 until r1.rows) {
            for(col <- 0 until r1.cols) {
              if(isNoData(result.get(col,row))) {
                (isNoData(r1.get(col,row)) ||
                 isNoData(r2.get(col,row)) ||
                 r2.get(col,row) == 0) should be (true)
              } else {
                result.get(col,row) should be (math.pow(r1.get(col,row), r2.get(col,row)))
              }
            }
          }
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }

    it("pow's three tiled RasterSources correctly") {
      val rs1 = createRasterSource(
        Array( 10,10,10, 10,10,10, 10,10,10,
               10,10,10, 10,10,10, 10,10,10,

               10,10,10, 10,10,10, 10,10,10,
               10,10,10, 10,10,10, 10,10,10),
        3,2,3,2)

      val rs2 = createRasterSource(
        Array( 3,3,3, 3,3,3, 3,3,3,
               3,3,3, 3,3,3, 3,3,3,

               3,3,3, 3,3,3, 3,3,3,
               3,3,3, 3,3,3, 3,3,3),
        3,2,3,2)

      val rs3 = createRasterSource(
        Array( 2,2,2, 2,2,2, 2,2,2,
               2,2,2, 2,2,2, 2,2,2,

               2,2,2, 2,2,2, 2,2,2,
               2,2,2, 2,2,2, 2,2,2),
        3,2,3,2)

      run(rs1**rs2**rs3) match {
        case Complete(result,success) =>
//          println(success)
          for(row <- 0 until 4) {
            for(col <- 0 until 9) {
              result.get(col,row) should be (math.pow(math.pow(10,3),2))
            }
          }
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }
  }
}
