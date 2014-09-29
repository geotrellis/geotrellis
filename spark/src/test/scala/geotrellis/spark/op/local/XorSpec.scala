/*
 * Copyright (c) 2014 DigitalGlobe.
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
 * See the License for the specific language governing permissions xor
 * limitations under the License.
 */

package geotrellis.spark.op.local

import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.rdd.RasterRDD
import geotrellis.spark.testfiles._

import org.scalatest.FunSpec

class XorSpec extends FunSpec
    with TestEnvironment
    with SharedSparkContext
    with RasterRDDMatchers
    with OnlyIfCanRunSpark {
  describe("Xor Operation") {
    ifCanRunSpark {
      val allOnes = AllOnesTestFile(inputHome, conf)
      val allTwos = AllTwosTestFile(inputHome, conf)
      val allHundreds = AllHundredsTestFile(inputHome, conf)

      it("should xor a raster with a constant") {
        val ones = sc.hadoopRasterRDD(allOnes.path)
        val res = ones ^ 1

        rasterShouldBe(res, (0, 0))
        rastersShouldHaveSameIdsAndTileCount(ones, res)
      }

      it("should xor a constant with a raster") {
        val ones = sc.hadoopRasterRDD(allOnes.path)
        val res = 2 ^: ones

        rasterShouldBe(res, (3, 3))
        rastersShouldHaveSameIdsAndTileCount(ones, res)
      }

      it("should xor three different rasters") {
        val ones = sc.hadoopRasterRDD(allOnes.path)
        val twos = sc.hadoopRasterRDD(allTwos.path)
        val hundreds = sc.hadoopRasterRDD(allHundreds.path)

        val res = ones ^ twos ^ hundreds

        rasterShouldBe(res, (103, 103))
        rastersShouldHaveSameIdsAndTileCount(ones, res)
      }

      it("should xor three different rasters as a seq") {
        val ones = sc.hadoopRasterRDD(allOnes.path)
        val twos = sc.hadoopRasterRDD(allTwos.path)
        val hundreds = sc.hadoopRasterRDD(allHundreds.path)

        val res = ones ^ Seq(twos, hundreds)

        rasterShouldBe(res, (103, 103))
        rastersShouldHaveSameIdsAndTileCount(ones, res)
      }
    }
  }
}
