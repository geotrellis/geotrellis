package geotrellis.raster.op.focal

import geotrellis._
import geotrellis.raster._

case class Conway(r:Op[Raster]) extends FocalOp[Raster,Int](r,Square(1)) {
  def createBuilder(r:Raster) = new ByteRasterBuilder(r.rasterExtent)
  def createCursor(r:Raster,n:Neighborhood) = Cursor.getInt(r,n)

  var count = 0

  def calc(cursor:Cursor[Int]) = {
    for(v <- cursor.addedCells) { if(v != NODATA) count += 1 }
    for(v <- cursor.removedCells) { if(v != NODATA) count -= 1 }
    if(count == 2 || count == 1) 1 else NODATA
  }
}
