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

package geotrellis.spark.summary.polygonal

import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.testkit.testfiles.{TestFiles ⇒ SparkTestFiles, _}
import geotrellis.raster.summary.polygonal._
import geotrellis.spark.testkit._

import geotrellis.raster._
import geotrellis.vector._

import org.scalatest.FunSpec

class MaxSpec extends FunSpec with TestEnvironment with SparkTestFiles {

  describe("Max Zonal Summary Operation") {
    val inc = IncreasingTestFile
    val multi = inc.withContext { _.mapValues { tile => MultibandTile(tile, tile) } }

    val tileLayout = inc.metadata.tileLayout
    val count = (inc.count * tileLayout.tileCols * tileLayout.tileRows).toInt
    val totalExtent = inc.metadata.extent

    val xd = totalExtent.xmax - totalExtent.xmin
    val yd = totalExtent.ymax - totalExtent.ymin

    val quarterExtent = Extent(
      totalExtent.xmin,
      totalExtent.ymin,
      totalExtent.xmin + xd / 2,
      totalExtent.ymin + yd / 2
    )

    it("should get correct max over whole raster extent") {
      inc.polygonalMax(totalExtent.toPolygon) should be(count - 1)
    }

    it("should get correct double max over whole raster extent for MultibandTileRDD") {
      multi.polygonalMax(totalExtent.toPolygon) map { _ should be(count - 1) }
    }

    it("should get correct max over a quarter of the extent") {
      val result = inc.polygonalMax(quarterExtent.toPolygon)
      val expected = inc.stitch.tile.polygonalMax(totalExtent, quarterExtent.toPolygon)

      result should be (expected)
    }

    it("should get correct double max over a quarter of the extent for MultibandTileRDD") {
      val result = multi.polygonalMax(quarterExtent.toPolygon)
      val expected = multi.stitch.tile.polygonalMax(totalExtent, quarterExtent.toPolygon)

      result.size should be (expected.size)

      result zip expected map { case (res, exp) =>
        res should be (exp)
      }
    }
  }

  describe("Max Zonal Summary Operation (collections api)") {
    val inc = IncreasingTestFile.toCollection
    val multi = IncreasingTestFile.withContext { _.mapValues { tile => MultibandTile(tile, tile) } }

    val tileLayout = inc.metadata.tileLayout
    val count = inc.length * tileLayout.tileCols * tileLayout.tileRows
    val totalExtent = inc.metadata.extent

    it("should get correct max over whole raster extent") {
      inc.polygonalMax(totalExtent.toPolygon) should be(count - 1)
    }

    it("should get correct double max over whole raster extent for MultibandTiles") {
      multi.polygonalMax(totalExtent.toPolygon) map { _ should be(count - 1) }
    }

    it("should get correct max over a quarter of the extent") {
      val xd = totalExtent.xmax - totalExtent.xmin
      val yd = totalExtent.ymax - totalExtent.ymin

      val quarterExtent = Extent(
        totalExtent.xmin,
        totalExtent.ymin,
        totalExtent.xmin + xd / 2,
        totalExtent.ymin + yd / 2
      )

      val result = inc.polygonalMax(quarterExtent.toPolygon)
      val expected = inc.stitch.tile.polygonalMax(totalExtent, quarterExtent.toPolygon)

      result should be (expected)
    }

    it("should get correct double max over a quarter of the extent for MultibandTiles") {
      val xd = totalExtent.xmax - totalExtent.xmin
      val yd = totalExtent.ymax - totalExtent.ymin

      val quarterExtent = Extent(
        totalExtent.xmin,
        totalExtent.ymin,
        totalExtent.xmin + xd / 2,
        totalExtent.ymin + yd / 2
      )

      val result = multi.polygonalMax(quarterExtent.toPolygon)
      val expected = multi.stitch.tile.polygonalMax(totalExtent, quarterExtent.toPolygon)

      result.size should be (expected.size)

      result zip expected map { case (res, exp) =>
        res should be (exp)
      }
    }
  }
}
