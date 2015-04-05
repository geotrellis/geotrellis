package geotrellis.raster.reproject

import geotrellis.raster._
import geotrellis.raster.interpolation._
import geotrellis.engine._
import geotrellis.testkit._
import geotrellis.proj4._
import geotrellis.raster.interpolation._
import geotrellis.raster.io.geotiff.reader._

import org.scalatest._
import spire.syntax.cfor._

class ModeReprojectSpec extends FunSpec
    with TileBuilders
    with TestEngine {

  describe("reprojects tile using mode in approximation to GDAL") {

    it ("should (approximately) match a GDAL mode interpolation on nlcd webmercator tile") {
      val GeoTiffBand(source, extent, crs, _) = GeoTiffReader
        .read("raster-test/data/reproject/nlcd_tile_wsg84.tif")
        .firstBand

      val GeoTiffBand(expected, expectedExtent, _, _) = GeoTiffReader
        .read("raster-test/data/reproject/nlcd_tile_webmercator-mode.tif")
        .firstBand

      val Raster(actual, actualExtent) =
        source.reproject(extent, crs, WebMercator, ReprojectOptions(Mode, 0.0))

      actual.rows should be (expected.rows)
      actual.cols should be (expected.cols)

      actualExtent.xmin should be (expectedExtent.xmin +- 0.00001)
      actualExtent.xmax should be (expectedExtent.xmax +- 0.00001)
      actualExtent.ymin should be (expectedExtent.ymin +- 0.00001)
      actualExtent.ymax should be (expectedExtent.ymax +- 0.00001)

      cfor(0)(_ < actual.rows - 1, _ + 1) { row =>
        cfor(0)(_ < actual.cols - 1, _ + 1) { col =>
          withClue(s"Failed on ($col, $row): ") {
            actual.getDouble(col, row) should be (expected.getDouble(col, row))
          }
        }
      }
    }

    it("should (approximately) match a GDAL mode on nlcd lat lon tile") {
      val GeoTiffBand(source, extent, crs, _) = GeoTiffReader
        .read("raster-test/data/reproject/nlcd_tile_wsg84.tif")
        .firstBand

      val GeoTiffBand(expected, expectedExtent, _, _) = GeoTiffReader
        .read("raster-test/data/reproject/nlcd_tile_latlon-mode.tif")
        .firstBand

      val Raster(actual, actualExtent) =
        source.reproject(extent, crs, LatLng, ReprojectOptions(Mode, 0.0))

      actual.rows should be (expected.rows)
      actual.cols should be (expected.cols)

      actualExtent.xmin should be (expectedExtent.xmin +- 0.00001)
      actualExtent.xmax should be (expectedExtent.xmax +- 0.00001)
      actualExtent.ymin should be (expectedExtent.ymin +- 0.00001)
      actualExtent.ymax should be (expectedExtent.ymax +- 0.00001)

      cfor(0)(_ < actual.rows - 1, _ + 1) { row =>
        cfor(0)(_ < actual.cols - 1, _ + 1) { col =>
          withClue(s"Failed on ($col, $row): ") {
            actual.getDouble(col, row) should be (expected.getDouble(col, row))
          }
        }
      }
    }

    it("should (approximately) match a GDAL mode on nlcd utm tile") {
      val GeoTiffBand(source, extent, crs, _) = GeoTiffReader
        .read("raster-test/data/reproject/nlcd_tile_wsg84.tif")
        .firstBand

      val GeoTiffBand(expected, expectedExtent, _, _) = GeoTiffReader
        .read("raster-test/data/reproject/nlcd_tile_utm-mode.tif")
        .firstBand

      val Raster(actual, actualExtent) =
        source.reproject(extent, crs, CRS.fromName("EPSG:32614"), ReprojectOptions(Mode, 0.0))

      actual.rows should be (expected.rows)
      actual.cols should be (expected.cols)

      actualExtent.xmin should be (expectedExtent.xmin +- 0.00001)
      actualExtent.xmax should be (expectedExtent.xmax +- 0.00001)
      actualExtent.ymin should be (expectedExtent.ymin +- 0.00001)
      actualExtent.ymax should be (expectedExtent.ymax +- 0.00001)

      cfor(0)(_ < actual.rows - 1, _ + 1) { row =>
        cfor(0)(_ < actual.cols - 1, _ + 1) { col =>
          withClue(s"Failed on ($col, $row): ") {
            actual.getDouble(col, row) should be (expected.getDouble(col, row))
          }
        }
      }
    }
  }
}

