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

import geotrellis.vector.Extent
import geotrellis.raster._
import geotrellis.raster.op.focal._
import geotrellis.testkit._
import geotrellis.engine._

import org.scalatest._

class SumSpec extends FunSpec with TestEngine with TileBuilders {

  describe("Sum") {

    it("should square sum r=1 for raster source") {
      val rs1 = createRasterSource(
        Array( nd,1,1,      1,1,1,      1,1,1,
                1,1,1,      2,2,2,      1,1,1,

                1,1,1,      3,3,3,      1,1,1,
                1,1,1,     1,nd,1,      1,1,1
        ),
        3,2,3,2
      )

      run(rs1.focalSum(Square(1))) match {
        case Complete(result,success) =>
//          println(success)
          assertEqual(result,
            Array(3, 5, 7,    8, 9, 8,    7, 6, 4,
                  5, 8,12,   15,18,15,   12, 9, 6,

                  6, 9,12,   14,17,14,   12, 9, 6,
                  4, 6, 8,    9,11, 9,    8, 6, 4))
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)

      }
    }

    it("should square sum with 5x5 neighborhood") {
      val rs1 = createRasterSource(
        Array( nd,1,1,      1,1,1,      1,1,1,
                1,1,1,      2,2,2,      1,1,1,

                1,1,1,      3,3,3,      1,1,1,
                1,1,1,     1,nd,1,      1,1,1
        ),
        3,2,3,2
      )

      run(rs1.focalSum(Square(2))) match {
        case Complete(result,success) =>
//          println(success)
          assertEqual(result,
            Array( 8, 14, 20,   24,24,24,    21,15, 9,
                  11, 18, 24,   28,28,28,    25,19,12,

                  11, 18, 24,   28,28,28,    25,19,12,
                   9, 15, 20,   23,23,23,    20,15, 9))
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)

      }
    }

    it("should circle sum for raster source") {
      val rs1 = createRasterSource(
        Array( nd,1,1,      1,1,1,      1,1,1,
                1,1,1,      2,2,2,      1,1,1,

                1,1,1,      3,3,3,      1,1,1,
                1,1,1,     1,nd,1,      1,1,1
        ),
        3,2,3,2
      )

      run(rs1.focalSum(Circle(1))) match {
        case Complete(result,success) =>
          //println(success)
          assertEqual(result,
            Array(2, 3, 4,    5, 5, 5,    4, 4, 3,
                  3, 5, 6,    9,10, 9,    6, 5, 4,

                  4, 5, 7,   10,11,10,    7, 5, 4,
                  3, 4, 4,    5, 5, 5,    4, 4, 3))
        case Error(msg,failure) =>
          // println(msg)
          // println(failure)
          assert(false)

      }
    }
  }
}
