package geotrellis.spark.tiling
import geotrellis.raster.TileLayout

object TmsTilingConvert {

  /*
   * Conversion methods from/to Geotrellis Tile Identification scheme (referred to as 
   * gtTileId below) which differs from geotrellis-spark's tileId scheme (referred to as 
   * tileId) in two important ways:
   * 
   * 1. gtTileIds start from upper left and go down to lower right whereas tileIds start 
   * from lower left and go to upper right
   * 2. gtTileIds go from 0 to tiles-1 
   * 3. gtTileIds and their corresponding gtTx,gtTy are Ints whereas tileIds, tx, ty are Long
   * 
   * TODO: The last point needs to be addressed  
   */
  def fromGtTileId(gtTileId: Int, layout: TileLayout, te: TileExtent, zoom: Int): Long = {
    val (gtTx, gtTy) = layout.getXY(gtTileId)
    val tx = te.xmin + gtTx
    val ty = te.ymax - gtTy
    TmsTiling.tileId(tx, ty, zoom)
  }
  def fromGtTileIdX(gtTx: Int, te: TileExtent): Long = te.xmin + gtTx

  def fromGtTileIdY(gtTy: Int, te: TileExtent): Long = te.ymax - gtTy

  def toGtTileId(tileId: Long, layout: TileLayout, te: TileExtent, zoom: Int): Int = {
    val (tx, ty) = TmsTiling.tileXY(tileId, zoom)
    val gtTx = (tx - te.xmin).toInt
    val gtTy = (te.ymax - ty).toInt
    layout.getTileIndex(gtTx, gtTy)
  }
  def toGtTileIdX(tx: Long, te: TileExtent): Int = (tx - te.xmin).toInt
  def toGtTileIdY(ty: Long, te: TileExtent): Int = (te.ymax - ty).toInt
}