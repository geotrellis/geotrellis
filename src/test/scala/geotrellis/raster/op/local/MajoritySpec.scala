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

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

import geotrellis.testutil._

class MajoritySpec extends FunSpec 
                  with ShouldMatchers 
                  with TestServer 
                  with RasterBuilders {
  describe("Majority") {
    it("takes majority on rasters of all one value") {
      val r1 = createRaster(Array.fill(7*8)(1), 7, 8)
      val r2 = createRaster(Array.fill(7*8)(5), 7, 8)
      val r3 = createRaster(Array.fill(7*8)(1), 7, 8)
      val r4 = createRaster(Array.fill(7*8)(7), 7, 8)
      val r5 = createRaster(Array.fill(7*8)(1), 7, 8)
      val r6 = createRaster(Array.fill(7*8)(7), 7, 8)
      val r7 = createRaster(Array.fill(7*8)(NODATA), 7, 8)

      assertEqual(Majority(r1,r2,r3,r4,r5,r6,r7), Array.fill(7*8)(1))
      assertEqual(Majority(1,r1,r2,r3,r4,r5,r6), Array.fill(7*8)(7))
      assertEqual(Majority(2,r1,r2,r3,r4,r5,r6), Array.fill(7*8)(5))
      assertEqual(Majority(0,r1,r1,r2), Array.fill(7*8)(1))
      assertEqual(Majority(1,r1,r1,r2), Array.fill(7*8)(5))
      assertEqual(Majority(2,r1,r1,r2), Array.fill(7*8)(NODATA))
      assertEqual(Majority(3,r1,r2,r3), Array.fill(7*8)(NODATA))
      assertEqual(Majority(4,r1,r2,r3), Array.fill(7*8)(NODATA))
    }

    it("takes majority on rasters sources of all one value") {
      val r1 = createRasterSource(Array.fill(6*8)(1), 2,2,3,4)
      val r2 = createRasterSource(Array.fill(6*8)(5), 2,2,3,4)
      val r3 = createRasterSource(Array.fill(6*8)(1), 2,2,3,4)
      val r4 = createRasterSource(Array.fill(6*8)(7), 2,2,3,4)
      val r5 = createRasterSource(Array.fill(6*8)(1), 2,2,3,4)
      val r6 = createRasterSource(Array.fill(6*8)(7), 2,2,3,4)
      val r7 = createRasterSource(Array.fill(6*8)(NODATA), 2,2,3,4)

      assertEqual(r1.localMajority(r2,r3,r4,r5,r6,r7).get, Array.fill(6*8)(1))
      assertEqual(r1.localMajority(1,r2,r3,r4,r5,r6).get, Array.fill(6*8)(7))
      assertEqual(r1.localMajority(2,r1,r2,r3,r4,r5,r6).get, Array.fill(6*8)(5))
      assertEqual(r1.localMajority(0,r1,r1,r2).get, Array.fill(6*8)(1))
      assertEqual(r1.localMajority(1,r1,r1,r2).get, Array.fill(6*8)(5))
      assertEqual(r1.localMajority(2,r1,r1,r2).get, Array.fill(6*8)(NODATA))
      assertEqual(r1.localMajority(3,r1,r2,r3).get, Array.fill(6*8)(NODATA))
      assertEqual(r1.localMajority(4,r1,r2,r3).get, Array.fill(6*8)(NODATA))
    }

    it("takes majority on double rasters sources of all one value") {
      val r1 = createRasterSource(Array.fill(6*8)(1.1), 2,2,3,4)
      val r2 = createRasterSource(Array.fill(6*8)(5.5), 2,2,3,4)
      val r3 = createRasterSource(Array.fill(6*8)(1.1), 2,2,3,4)
      val r4 = createRasterSource(Array.fill(6*8)(7.7), 2,2,3,4)
      val r5 = createRasterSource(Array.fill(6*8)(1.1), 2,2,3,4)
      val r6 = createRasterSource(Array.fill(6*8)(7.7), 2,2,3,4)
      val r7 = createRasterSource(Array.fill(6*8)(NaN), 2,2,3,4)

      assertEqual(r1.localMajority(r2,r3,r4,r5,r6,r7).get, Array.fill(6*8)(1.1))
      assertEqual(r1.localMajority(1,r2,r3,r4,r5,r6).get, Array.fill(6*8)(7.7))
      assertEqual(r1.localMajority(2,r1,r2,r3,r4,r5,r6).get, Array.fill(6*8)(5.5))
      assertEqual(r1.localMajority(0,r1,r1,r2).get, Array.fill(6*8)(1.1))
      assertEqual(r1.localMajority(1,r1,r1,r2).get, Array.fill(6*8)(5.5))
      assertEqual(r1.localMajority(2,r1,r1,r2).get, Array.fill(6*8)(NaN))
      assertEqual(r1.localMajority(3,r1,r2,r3).get, Array.fill(6*8)(NaN))
      assertEqual(r1.localMajority(4,r1,r2,r3).get, Array.fill(6*8)(NaN))
    }
  }
}
