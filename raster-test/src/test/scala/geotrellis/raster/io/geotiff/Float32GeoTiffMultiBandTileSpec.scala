package geotrellis.raster.io.geotiff

import geotrellis.raster._
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.op.local._

import geotrellis.vector.Extent

import geotrellis.proj4._

import geotrellis.testkit._

import org.scalatest._

class Float32GeoTiffMultibandTileSpec extends FunSpec
    with Matchers
    with BeforeAndAfterAll
    with TestEngine
    with GeoTiffTestUtils 
    with TileBuilders {
  def p(s: String, i: String): String = 
    geoTiffPath(s"3bands/float32/3bands-${s}-${i}.tif")

  describe("Float32GeoTiffMultibandTile") {

    // Combine all bands, int

    it("should combine all bands with pixel interleave, striped") {
      val tile =
        MultibandGeoTiff.compressed(p("striped", "pixel")).tile

      val actual = tile.combine(_.sum)
      val expected = FloatArrayTile(Array.ofDim[Float](tile.cols * tile.rows).fill(6), tile.cols, tile.rows)

      assertEqual(actual, expected)
    }

    it("should combine all bands with pixel interleave, tiled") {
      val tile =
        MultibandGeoTiff.compressed(p("tiled", "pixel")).tile

      val actual = tile.combine(_.sum)
      val expected = FloatArrayTile(Array.ofDim[Float](tile.cols * tile.rows).fill(6), tile.cols, tile.rows)

      assertEqual(actual, expected)
    }

    it("should combine all bands with band interleave, striped") {
      val tile =
        MultibandGeoTiff.compressed(p("striped", "band")).tile

      val actual = tile.combine(_.sum)
      val expected = FloatArrayTile(Array.ofDim[Float](tile.cols * tile.rows).fill(6), tile.cols, tile.rows)

      assertEqual(actual, expected)
    }

    it("should combine all bands with band interleave, tiled") {
      val tile =
        MultibandGeoTiff.compressed(p("tiled", "band")).tile

      val actual = tile.combine(_.sum)
      val expected = FloatArrayTile(Array.ofDim[Float](tile.cols * tile.rows).fill(6), tile.cols, tile.rows)

      assertEqual(actual, expected)
    }
  }
}
