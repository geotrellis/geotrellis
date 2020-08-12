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

package geotrellis.raster.gdal

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.reproject.{Reproject, ReprojectRasterExtent}
import geotrellis.raster.testkit._

import org.scalatest.GivenWhenThen
import org.scalatest.funspec.AnyFunSpec

class GDALReprojectRasterSourceSpec extends AnyFunSpec with RasterMatchers with GivenWhenThen {

  /**
    * Pipeline to generate test dataset from the aspect-tiled.tif.
    * Example below is for output format VRT, however at the moment we use in memory rasters (-of MEM),
    * basically this line can be generated by printing GDALWarpOptions:
    * gdalwarp -of VRT -r bilinear -et 0.125 -tap -tr 1.1205739005262034E-4 1.1205739005262034E-4 -s_srs '+proj=lcc +lat_1=36.16666666666666 +lat_2=34.33333333333334 +lat_0=33.75 +lon_0=-79 +x_0=609601.22 +y_0=0 +datum=NAD83 +units=m +no_defs'  -t_srs '+proj=longlat +datum=WGS84 +no_defs'
    *
    * The line above will produce a VRT file (all files are present there at the moment).
    * To translate it into tiff use the next command:
    * gdal_translate in.vrt out.tif
    *
    * The output file would be a warped result.
    * */

  describe("Reprojecting a RasterSource") {

    val uri = Resource.path("vlm/aspect-tiled.tif")

    /**
      * For some reasons, the Pipeline described above is OS specific,
      * and Bilinear interpolation behaves differently.
      * To make tests pass there was generated one bilinear version under mac and anther inside a linux container.
      *
      * TODO: investigate the nature of this bug later
      * */

    val expectedUri = Map[ResampleMethod, String](
      Bilinear -> {
        if(System.getProperty("os.name").toLowerCase().startsWith("mac"))
          Resource.path("vlm/aspect-tiled-bilinear.tif")
        else
          Resource.path("vlm/aspect-tiled-bilinear-linux.tif")
      },
      NearestNeighbor ->  Resource.path("vlm/aspect-tiled-near.tif")
    )

    def testReprojection(method: ResampleMethod) = {
      val rasterSource = GDALRasterSource(uri)
      val expectedRasterSource = GDALRasterSource(expectedUri(method))
      val expectedRasterExtent = expectedRasterSource.gridExtent.toRasterExtent
      val warpRasterSource = rasterSource.reprojectToRegion(LatLng, expectedRasterExtent, method)
      val testBounds = GridBounds(0, 0, expectedRasterExtent.cols, expectedRasterExtent.rows).split(64,64).toSeq
      val transform = Transform(rasterSource.crs, warpRasterSource.crs)

      warpRasterSource.resolutions.size shouldBe rasterSource.resolutions.size
      rasterSource.resolutions.zip(warpRasterSource.resolutions).map { case (scz, CellSize(ew, eh)) =>
        val CellSize(cw, ch) = ReprojectRasterExtent(GridExtent[Long](rasterSource.extent, scz), transform, Reproject.Options.DEFAULT).cellSize
        cw shouldBe ew +- 1e-4
        ch shouldBe eh +- 1e-4
      }

      for (bound <- testBounds) yield {
        withClue(s"Read window ${bound}: ") {
          val targetExtent = expectedRasterExtent.extentFor(bound)
          val testRasterExtent = RasterExtent(
            extent     = targetExtent,
            cellwidth  = expectedRasterExtent.cellwidth,
            cellheight = expectedRasterExtent.cellheight,
            cols       = bound.width,
            rows       = bound.height
          )

          // due to a bit different logic used by GDAL working with different output formats
          // there can be a difference around +-1e-11
          val expected = Utils.roundRaster(expectedRasterSource.read(testRasterExtent.extent).get)
          val actual = Utils.roundRaster(warpRasterSource.read(bound.toGridType[Long]).get)

          actual.extent.covers(expected.extent) should be (true) // -- doesn't work due to a precision issue
          actual.rasterExtent.extent.xmin should be (expected.rasterExtent.extent.xmin +- 1e-5)
          actual.rasterExtent.extent.ymax should be (expected.rasterExtent.extent.ymax +- 1e-5)
          actual.rasterExtent.cellwidth should be (expected.rasterExtent.cellwidth +- 1e-5)
          actual.rasterExtent.cellheight should be (expected.rasterExtent.cellheight +- 1e-5)

          withGeoTiffClue(actual, expected, LatLng)  {
            assertRastersEqual(actual, expected)
          }
        }
      }
    }

    it("should reproject using NearestNeighbor") {
      testReprojection(NearestNeighbor)
    }

    it("should reproject using Bilinear") {
      testReprojection(Bilinear)
    }
  }
}
