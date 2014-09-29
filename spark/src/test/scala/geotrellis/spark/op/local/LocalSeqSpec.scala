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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.op.local

import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.rdd.RasterRDD
import geotrellis.spark.testfiles._

import org.scalatest.FunSpec

class LocalSeqSpec extends FunSpec
    with TestEnvironment
    with SharedSparkContext
    with RasterRDDMatchers
    with OnlyIfCanRunSpark {
  describe("Local Seq Operations") {
    ifCanRunSpark {
      val allOnes = AllOnesTestFile(inputHome, conf)
      val allTwos = AllTwosTestFile(inputHome, conf)
      val allHundreds = AllHundredsTestFile(inputHome, conf)
      val increasing = IncreasingTestFile(inputHome, conf)
      val decreasing = DecreasingTestFile(inputHome, conf)

      val cols = allOnes.metaData.cols
      val rows = allOnes.metaData.rows

      it("should test raster rdd seq with one element") {
        val ones = sc.hadoopRasterRDD(allOnes.path)

        val res = Seq(ones).localAdd

        rasterShouldBe(res, (1, 1))
        rastersShouldHaveSameIdsAndTileCount(ones, res)
      }

      it("should add rasters") {
        val ones = sc.hadoopRasterRDD(allOnes.path)
        val hundreds = sc.hadoopRasterRDD(allHundreds.path)

        val res = Seq(ones, hundreds, ones).localAdd

        rasterShouldBe(res, (102, 102))
        rastersShouldHaveSameIdsAndTileCount(ones, res)
      }

      it("should get variety of rasters") {
        val ones = sc.hadoopRasterRDD(allOnes.path)
        val hundreds = sc.hadoopRasterRDD(allHundreds.path)

        val res = Seq(ones, hundreds, ones).localVariety

        rasterShouldBe(res, (2, 2))
        rastersShouldHaveSameIdsAndTileCount(ones, res)
      }

      it("should get mean of rasters") {
        val ones = sc.hadoopRasterRDD(allOnes.path)
        val hundreds = sc.hadoopRasterRDD(allHundreds.path)

        val res = Seq(ones, hundreds, ones).localMean

        rasterShouldBe(res, (34, 34))
        rastersShouldHaveSameIdsAndTileCount(ones, res)
      }

      it("should min three rasters as a seq") {
        val inc = sc.hadoopRasterRDD(increasing.path)
        val dec = sc.hadoopRasterRDD(decreasing.path)
        val hundreds = sc.hadoopRasterRDD(allHundreds.path)

        val res = Seq(inc, dec, hundreds).localMin

        rasterShouldBe(
          res,
          (x: Int, y: Int) => {
            val decV = cols * rows - (y * cols + x) - 1
            val incV = y * cols + x

            math.min(math.min(decV, incV), 100)
          }
        )

        rastersShouldHaveSameIdsAndTileCount(inc, res)
      }

      it("should min three rasters as a seq and take n:th smallest") {
        val inc = sc.hadoopRasterRDD(increasing.path)
        val dec = sc.hadoopRasterRDD(decreasing.path)
        val hundreds = sc.hadoopRasterRDD(allHundreds.path)

        val res = Seq(inc, dec, hundreds).localMinN(1)

        rasterShouldBe(
          res,
          (x: Int, y: Int) => {
            val decV = cols * rows - (y * cols + x) - 1
            val incV = y * cols + x

            val d = Array(decV, incV, 100).sorted
            d(1)
          }
        )

        rastersShouldHaveSameIdsAndTileCount(inc, res)
      }

      it("should max three rasters as a seq") {
        val inc = sc.hadoopRasterRDD(increasing.path)
        val dec = sc.hadoopRasterRDD(decreasing.path)
        val hundreds = sc.hadoopRasterRDD(allHundreds.path)

        val res = Seq(inc, dec, hundreds).localMax

        rasterShouldBe(
          res,
          (x: Int, y: Int) => {
            val decV = cols * rows - (y * cols + x) - 1
            val incV = y * cols + x

            math.max(math.max(decV, incV), 100)
          }
        )

        rastersShouldHaveSameIdsAndTileCount(inc, res)
      }

      it("should max three rasters as a seq and take n:th smallest") {
        val inc = sc.hadoopRasterRDD(increasing.path)
        val dec = sc.hadoopRasterRDD(decreasing.path)
        val hundreds = sc.hadoopRasterRDD(allHundreds.path)

        val res = Seq(inc, dec, hundreds).localMaxN(1)

        rasterShouldBe(
          res,
          (x: Int, y: Int) => {
            val decV = cols * rows - (y * cols + x) - 1
            val incV = y * cols + x

            val d = Array(decV, incV, 100).sorted
            d(1)
          }
        )

        rastersShouldHaveSameIdsAndTileCount(inc, res)
      }

      it("should assign the minority of each raster") {
        val ones = sc.hadoopRasterRDD(allOnes.path)
        val twos = sc.hadoopRasterRDD(allTwos.path)
        val hundreds = sc.hadoopRasterRDD(allHundreds.path)

        val res = Seq(ones, twos, twos, hundreds, hundreds).localMinority()

        rasterShouldBe(res, (1, 1))
        rastersShouldHaveSameIdsAndTileCount(res, ones)
      }

      it("should assign the nth minority of each raster") {
        val ones = sc.hadoopRasterRDD(allOnes.path)
        val twos = sc.hadoopRasterRDD(allTwos.path)
        val hundreds = sc.hadoopRasterRDD(allHundreds.path)

        val res = Seq(ones, twos, twos, twos, hundreds, hundreds).localMinority(1)

        rasterShouldBe(res, (100, 100))
        rastersShouldHaveSameIdsAndTileCount(res, ones)
      }

      it("should assign the majority of each raster") {
        val ones = sc.hadoopRasterRDD(allOnes.path)
        val twos = sc.hadoopRasterRDD(allTwos.path)
        val hundreds = sc.hadoopRasterRDD(allHundreds.path)

        val res = Seq(ones, ones, ones, twos, twos, hundreds).localMajority()

        rasterShouldBe(res, (1, 1))
        rastersShouldHaveSameIdsAndTileCount(res, ones)
      }

      it("should assign the nth majority of each raster") {
        val ones = sc.hadoopRasterRDD(allOnes.path)
        val twos = sc.hadoopRasterRDD(allTwos.path)
        val hundreds = sc.hadoopRasterRDD(allHundreds.path)

        val res = Seq(ones, ones, ones, twos, twos, hundreds).localMajority(1)

        rasterShouldBe(res, (2, 2))
        rastersShouldHaveSameIdsAndTileCount(res, ones)
      }
    }
  }
}
