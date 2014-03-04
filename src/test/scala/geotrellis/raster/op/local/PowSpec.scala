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
import geotrellis.source._
import geotrellis.process._

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

import geotrellis.testutil._

class PowSpec extends FunSpec 
                      with ShouldMatchers 
                      with TestServer 
                      with RasterBuilders {
  describe("Pow") {
    it("Raises an int raster to an int power") {
      val r = positiveIntegerRaster
      val result = get(Pow(r,5))
      for(col <- 0 until r.cols) {
        for(row <- 0 until r.rows) {
          withClue(s"Failure at $col,$row") {
            result.get(col,row) should be (math.pow(r.get(col,row),5).toInt)
          }
        }
      }
    }

    it("raises a double raster to an int power") {
      val r = probabilityRaster
      val result = get(Pow(r,3))
      for(col <- 0 until r.cols) {
        for(row <- 0 until r.rows) {
          result.getDouble(col,row) should be (math.pow(r.getDouble(col,row),3))
        }
      }
    }

    it("raises an integer to the power of an int raster's cells") {
      val r = positiveIntegerRaster
      val result = get(Pow(-10,r))
      for(col <- 0 until r.cols) {
        for(row <- 0 until r.rows) {
          result.get(col,row) should be (math.pow(-10, r.get(col,row)).toInt)
        }
      }
    }

    it("raises an integer to the power of a double raster's cells") {
      val r = probabilityRaster
      val result = get(Pow(3,r))
      for(col <- 0 until r.cols) {
        for(row <- 0 until r.rows) {
          result.getDouble(col,row) should be (math.pow(3.0, r.getDouble(col,row)))
        }
      }
    }

    it("raises an int raster to a double power") {
      val r = positiveIntegerRaster
      val result = get(Pow(r,5.1))
      for(col <- 0 until r.cols) {
        for(row <- 0 until r.rows) {
          result.get(col,row) should be (math.pow(r.get(col,row), 5.1).toInt)
        }
      }
    }

    it("raises a double raster to a double power") {
      val r = probabilityRaster
      val result = get(Pow(r,.3))
      for(col <- 0 until r.cols) {
        for(row <- 0 until r.rows) {
          result.getDouble(col,row) should be (math.pow(r.getDouble(col,row), 0.3))
        }
      }
    }

    it("raises a double to the power of a int raster's cells") {
      val r = positiveIntegerRaster
      val result = get(Pow(-10.7,r))
      for(col <- 0 until r.cols) {
        for(row <- 0 until r.rows) {
          result.get(col,row) should be ( math.pow(-10.7, r.get(col,row)).toInt)
        }
      }
    }

    it("raises a double value to the power of a double raster's cells") {
      val r = probabilityRaster
      val result = get(Pow(-3.3,r))
      for(col <- 0 until r.cols) {
        for(row <- 0 until r.rows) {
          val z = r.getDouble(col,row)
          val rz = result.getDouble(col,row)
          if(isNoData(z) || z < 1)
            withClue(s"z = $z, rz = $rz") { isNoData(rz) should be (true) }
          else
            withClue(s"z = $z, rz = $rz") { rz should be (math.pow(-3.3, r.getDouble(col,row))) }
        }
      }
    }

    it("raises an integer raster to itself") {
      val r = positiveIntegerRaster
      val result = get(Pow(r,r))
      for(col <- 0 until r.cols) {
        for(row <- 0 until r.rows) {
          val z = r.get(col,row)
          val rz = result.get(col,row)
          withClue(s"$z ** $z != $rz") { rz should be (math.pow(z,z).toInt) }
        }
      }
    }
    
    it("raises a double raster to itself") {
      val r = probabilityRaster
      val result = get(Pow(r,r))
      for(col <- 0 until r.cols) {
        for(row <- 0 until r.rows) {
          val z = r.getDouble(col,row)
          result.getDouble(col,row) should be (math.pow(z,z))
        }
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
          for(row <- 0 until r1.rasterExtent.rows) {
            for(col <- 0 until r1.rasterExtent.cols) {
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
