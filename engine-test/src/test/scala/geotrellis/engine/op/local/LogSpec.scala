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


class LogSpec extends FunSpec with Matchers with TestEngine {
  describe("Log") {    
    it("takes log of int tiled RasterSource") {
      val rs = createRasterSource(
        Array( NODATA,20,20, 20,20,20, 20,20,20,
               20,20,20, 20,20,20, 20,20,20,

               20,20,20, 20,20,20, 20,20,20,
               20,20,20, 20,20,20, 20,20,20),
        3,2,3,2)

      run(rs.localLog) match {
        case Complete(result,success) =>
//          println(success)
          for(row <- 0 until 4) {
            for(col <- 0 until 9) {
              if(row == 0 && col == 0)
                result.get(col,row) should be (NODATA)
              else
                result.get(col,row) should be (math.log(20).toInt)
            }
          }
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }

    it("takes log of Double tiled RasterSource") {
      val rs = createRasterSource(
        Array( Double.NaN,34.2,34.2, 34.2,34.2,34.2, 34.2,34.2,34.2,
               34.2,34.2,34.2, 34.2,34.2,34.2, 34.2,34.2,34.2,

               34.2,34.2,34.2, 34.2,34.2,34.2, 34.2,34.2,34.2,
               34.2,34.2,34.2, 34.2,34.2,34.2, 34.2,34.2,34.2),
        3,2,3,2)

      run(rs.localLog) match {
        case Complete(result,success) =>
//          println(success)
          for(row <- 0 until 4) {
            for(col <- 0 until 9) {
              if(row == 0 && col == 0)
                isNoData(result.getDouble(col,row)) should be (true)
              else
                result.getDouble(col,row) should be (math.log(34.2))
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
