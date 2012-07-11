package geotrellis.op.raster

import geotrellis._
import geotrellis.op._
import geotrellis.process._
import geotrellis.data.RasterReader

/**
 * This uses a nearest-neighbor algorithm to resample a raster.
 */
case class ResampleRaster(r:Op[Raster], cols:Op[Int], rows:Op[Int]) 
extends Op3(r,cols,rows)({
  (raster,cols,rows) => {
    val extent = raster.rasterExtent.extent
    val cw = extent.width / cols
    val ch = extent.height / rows
    val re = RasterExtent(extent, cw, ch, cols, rows)
    Result(RasterReader.read(raster, Option(re)))
  }
})
