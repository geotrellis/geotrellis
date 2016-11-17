/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.mapalgebra.local

import geotrellis.spark._
import geotrellis.proj4._
import geotrellis.raster.Tile
import geotrellis.spark.tiling._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.mapalgebra.Implicits._
import geotrellis.spark.testfiles._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSpec

class AddSpec extends FunSpec with TestEnvironment with TestFiles with KryoEncoderImplicits {
  
  describe("Add Operation") {
    import ssc.implicits._

    val ones = AllOnesTestFile
    val onesST = AllOnesSpaceTime

    val onesDS = AllOnesTestFile.toDS()
    val onesSTDS = AllOnesSpaceTime.toDS()

    it("should add a constant to a raster") {
      val twos = ones + 1

      rasterShouldBe(twos, (2, 2))
      rastersShouldHaveSameIdsAndTileCount(ones, twos)
    }

    it("should add a constant to a raster (dataset)") {
      val twos = (onesDS + 1).rdd

      rasterShouldBe(twos, (2, 2))
      rastersShouldHaveSameIdsAndTileCount(ones, twos)
    }

    it("should add a constant to a spacetime raster") {
      val twos = onesST + 1

      rasterShouldBe(twos, (2, 2))
      rastersShouldHaveSameIdsAndTileCount(onesST, twos)
    }

    it("should add a constant to a spacetime raster (dataset)") {
      val twos = (onesSTDS + 1).rdd

      rasterShouldBe(twos, (2, 2))
      rastersShouldHaveSameIdsAndTileCount(onesST, twos)
    }

    it("should add a raster to a constant") {
      val twos = 1 +: ones

      rasterShouldBe(twos, (2, 2))
      rastersShouldHaveSameIdsAndTileCount(ones, twos)
    }

    it("should add a raster to a constant (dataset)") {
      val twos = (1 +: onesDS).rdd

      rasterShouldBe(twos, (2, 2))
      rastersShouldHaveSameIdsAndTileCount(ones, twos)
    }


    it("should add a spacetime raster to a constant") {
      val twos = 1 +: onesST

      rasterShouldBe(twos, (2, 2))
      rastersShouldHaveSameIdsAndTileCount(onesST, twos)
    }

    it("should add a spacetime raster to a constant (dataset)") {
      val twos = (1 +: onesSTDS).rdd

      rasterShouldBe(twos, (2, 2))
      rastersShouldHaveSameIdsAndTileCount(onesST, twos)
    }

    it("should add multiple rasters") {
      val threes = ones + ones + ones

      rasterShouldBe(threes, (3, 3))
      rastersShouldHaveSameIdsAndTileCount(ones, threes)
    }

    it("should add multiple rasters (dataset)") {
      val threes = ((onesDS + onesDS).rdd.toDS() + onesDS).rdd // Datasets serialization issues(?)

      rasterShouldBe(threes, (3, 3))
      rastersShouldHaveSameIdsAndTileCount(ones, threes)
    }

    it("should add multiple spacetime rasters") {
      val twos = onesST + onesST

      rasterShouldBe(twos, (2, 2))
      rastersShouldHaveSameIdsAndTileCount(onesST, twos)
    }

    it("should add multiple spacetime rasters (dataset)") {
      val twos = (onesSTDS + onesSTDS).rdd

      rasterShouldBe(twos, (2, 2))
      rastersShouldHaveSameIdsAndTileCount(onesST, twos)
    }

    it("should add multiple rasters as a seq") {
      val threes = ones + Array(ones, ones)

      rasterShouldBe(threes, (3, 3))
      rastersShouldHaveSameIdsAndTileCount(ones, threes)
    }

    it("should add multiple rasters as a seq (dataset)") {
      val threes = (onesDS + Array(onesDS, onesDS)).rdd

      rasterShouldBe(threes, (3, 3))
      rastersShouldHaveSameIdsAndTileCount(ones, threes)
    }

    it("should add multiple spacetime rasters as a seq") {
      val twos = onesST + Array(onesST)

      rasterShouldBe(twos, (2, 2))
      rastersShouldHaveSameIdsAndTileCount(onesST, twos)
    }

    it("should add multiple spacetime rasters as a seq (dataset)") {
      val twos = (onesSTDS + Array(onesSTDS)).rdd

      rasterShouldBe(twos, (2, 2))
      rastersShouldHaveSameIdsAndTileCount(onesST, twos)
    }
  }
}
