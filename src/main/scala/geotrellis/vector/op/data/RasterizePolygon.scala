package geotrellis.vector.op.data

import geotrellis._
import geotrellis.geometry.Polygon
import geotrellis.geometry.rasterizer.Rasterizer
import geotrellis.process._
import geotrellis._


/**
 * Rasterize a polygon and then draw it on the provided raster.
 */
case class RasterizePolygon(r:Op[Raster], p:Op[Polygon]) extends Op2(r,p) ({
  (raster,polygon) => {
    val copy = raster.copy()
    Rasterizer.rasterize(copy, Array(polygon))
    Result(copy)
  }
})
