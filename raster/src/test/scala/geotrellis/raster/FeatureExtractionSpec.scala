/*
 * Copyright 2019 Azavea
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

package geotrellis.raster

import geotrellis.vector._
import geotrellis.raster.testkit._

import org.scalatest._

class FeatureExtractionSpec extends FunSpec
                  with Matchers
                  with RasterMatchers
                  with TileBuilders {
  describe("Tile") {
    it("should extract all int point features") {
      val ext = Extent(0.0, 0.0, 3.0, 3.0)
      val data = Array(
        1, 2, 3,
        4, 5, 6,
        7, 8, 9
      )
      val raster = Raster(ArrayTile(data, 3, 3), ext)

      val features = raster.pointFeatures[Int](ext.toPolygon)

      features.map(_.data) shouldBe data
      features.foreach { case feature @ Feature(point, _) => raster.pointFeatures[Int](point).head shouldBe feature }
    }

    it("should extract all double point features") {
      val ext = Extent(0.0, 0.0, 3.0, 3.0)
      val data = Array(
        1.1, 2.2, 3.3,
        4.4, 5.5, 6.6,
        7.7, 8.8, 9.8
      )
      val raster = Raster(ArrayTile(data, 3, 3), ext)

      val features = raster.pointFeatures[Double](ext.toPolygon)

      features.map(_.data) shouldBe data
      features.foreach { case feature @ Feature(point, _) => raster.pointFeatures[Double](point).head shouldBe feature }
    }
  }

  describe("MultibandTile") {
    it("should extract all int point features") {
      val ext = Extent(0.0, 0.0, 3.0, 3.0)
      val b1 = Array(
        1, 2, 3,
        4, 5, 6,
        7, 8, 9
      )
      val b2 = b1.map(_ + 1)
      val b3 = b2.map(_ + 1)
      val data = Array(b1, b2, b3)
      val raster = Raster(MultibandTile(data.map(ArrayTile(_, 3, 3))), ext)

      val features = raster.pointFeatures[Int](ext.toPolygon)

      features.map(_.map(_.data)) shouldBe data
      features.zipWithIndex.foreach { case (features, i) =>
        features.foreach { case feature @ Feature(point, _) =>
          raster.pointFeatures[Int](point).apply(i).head shouldBe feature }
      }
    }

    it("should extract all double point features") {
      val ext = Extent(0.0, 0.0, 3.0, 3.0)
      val b1 = Array(
        1.1, 2.2, 3.3,
        4.4, 5.5, 6.6,
        7.7, 8.8, 9.8
      )
      val b2 = b1.map(_ + 1)
      val b3 = b2.map(_ + 1)
      val data = Array(b1, b2, b3)
      val raster = Raster(MultibandTile(data.map(ArrayTile(_, 3, 3))), ext)

      val features = raster.pointFeatures[Double](ext.toPolygon)

      features.map(_.map(_.data)) shouldBe data
      features.zipWithIndex.foreach { case (features, i) =>
        features.foreach { case feature @ Feature(point, _) =>
          raster.pointFeatures[Double](point).apply(i).head shouldBe feature }
      }
    }
  }
}
