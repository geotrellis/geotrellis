package geotrellis.spark.op.global

import geotrellis.spark._

import geotrellis.raster.op.global._
import geotrellis.raster._

import org.scalatest.FunSpec

class VerticalFlipSpec extends FunSpec with TestEnvironment {

  describe("VerticalFlip Global Spec") {
    it("should perform as the non-distributed raster operation") {
      val rasterOp: (Tile, RasterExtent) => Tile = (tile: Tile, re: RasterExtent) => tile.verticalFlip()
      val sparkOp: RasterRDD[SpatialKey] => RasterRDD[SpatialKey] = (rdd: RasterRDD[SpatialKey]) => rdd.verticalFlip()

      val path = "aspect.tif"

      testGeoTiff(sc, path)(rasterOp, sparkOp)
    }

  }
}
