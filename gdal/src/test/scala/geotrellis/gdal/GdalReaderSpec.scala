package geotrellis.gdal

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.proj4._

import org.scalatest._

class GdalReaderSpec extends FunSpec with Matchers {
  val path = "raster-test/data/slope.tif"

  describe("reading a GeoTiff") {
    it("should match one read with GeoTools") {
      println("Reading with GDAL...")
      val (gdRaster, RasterExtent(gdExt,_, _, _, _)) = GdalReader.read(path)
      println("Reading with GeoTools....")
      val Raster(gtRaster, gtExt) = SinglebandGeoTiff(path).raster
      println("Done.")

      gdExt.xmin should be (gtExt.xmin +- 0.00001)
      gdExt.xmax should be (gtExt.xmax +- 0.00001)
      gdExt.ymin should be (gtExt.ymin +- 0.00001)
      gdExt.ymax should be (gtExt.ymax +- 0.00001)

      gdRaster.cols should be (gtRaster.cols)
      gdRaster.rows should be (gtRaster.rows)

      gdRaster.cellType should be (gtRaster.cellType)

      println("Comparing rasters...")
      for(col <- 0 until gdRaster.cols) {
        for(row <- 0 until gdRaster.rows) {
          val actual = gdRaster.getDouble(col, row)
          val expected = gtRaster.getDouble(col, row)
          withClue(s"At ($col, $row): GDAL - $actual  GeoTools - $expected") {
            isNoData(actual) should be (isNoData(expected))
            if(isData(actual))
              actual should be (expected)
          }
        }
      }
    }

    it("should read CRS from file") {
      val rasterDataSet = Gdal.open("raster-test/data/geotiff-test-files/all-ones.tif")
      rasterDataSet.crs should equal (Some(LatLng))
    }
  }
}
