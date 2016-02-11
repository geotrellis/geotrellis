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

import geotrellis.engine._
import geotrellis.raster._

import org.scalatest._


class AtanSpec extends FunSpec with Matchers with TestEngine {

  describe("ArcTan") {
    it("finds arctan of a double raster") {
      val rasterData = Array(
         0.0, 1/math.sqrt(3), 1.0,
         math.sqrt(3), Double.PositiveInfinity, Double.NaN,

        -0.0,-1/math.sqrt(3),-1.0,
        -math.sqrt(3), Double.NegativeInfinity, Double.NaN,

         0.0, 1/math.sqrt(3), 1.0,
         math.sqrt(3), Double.PositiveInfinity, Double.NaN,

        -0.0,-1/math.sqrt(3),-1.0,
        -math.sqrt(3), Double.NegativeInfinity, Double.NaN
      )
      val rs = createRasterSource(rasterData, 3, 2, 2, 2)
      val expectedAngles = List( 0.0,    1.0/6,  1.0/4,
                                 1.0/3,  0.5,    Double.NaN,

                                -0.0,   -1.0/6, -1.0/4,
                                -1.0/3, -0.5,   -Double.NaN,

                                 0.0,    1.0/6,  1.0/4,
                                 1.0/3,  0.5,    Double.NaN,

                                -0.0,   -1.0/6, -1.0/4,
                                -1.0/3, -0.5,   -Double.NaN
                           ).map(x => math.Pi * x)
      run(rs.localAtan) match {
        case Complete(result, success) =>
          val width = 6
          val height = 4
          val len = expectedAngles.length
          for (y <- 0 until height) {
            for (x <- 0 until width) {
              val angle = result.getDouble(x, y)
              val i = (y*width + x) % len
              val expected = expectedAngles(i)
              val epsilon = math.ulp(angle)
              if (x == 5) {
                angle.isNaN should be (true)
              } else {
                angle should be (expected +- epsilon)
              }
            }
          }
        case Error(msg, failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }

    it("finds arctan of an int raster") {
      val rasterData = Array(
         0, 0, 0,   0, 0, 0,   0, 0, 0,
         1, 1, 1,   1, 1, 1,   1, 1, 1,
        -1,-1,-1,  -1,-1,-1,  -1,-1,-1,

         2, 2, 2,   2, 2, 2,   2, 2, 2,
        -2,-2,-2,  -2,-2,-2,  -2,-2,-2,
        NODATA, NODATA, NODATA, NODATA, NODATA, NODATA, NODATA, NODATA, NODATA
      )
      val expectedAngles = Array(0.0, 0.25*math.Pi, -0.25*math.Pi,
                                 math.atan(2), math.atan(-2),  Double.NaN)
      val rs = createRasterSource(rasterData, 3, 3, 3, 2)
      run(rs.localAtan) match {
        case Complete(result, success) =>
          for (y <- 0 until 5) {
            for (x <- 0 until 9) {
              val cellValue = result.getDouble(x, y)
              val epsilon = math.ulp(cellValue)
              val expected = expectedAngles(y)
              cellValue should be (expected +- epsilon)
            }
          }
          for (y <- 5 until 6) {
            for (x <- 0 until 9) {
              result.getDouble(x, y).isNaN should be (true)
            }
          }
        case Error(msg, failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }
  }
}
