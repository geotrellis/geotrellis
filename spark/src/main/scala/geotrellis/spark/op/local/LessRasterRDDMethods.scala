package geotrellis.spark.op.local

import geotrellis.spark._
import geotrellis.raster.op.local.Less
import geotrellis.spark.rdd.RasterRDD

trait LessRasterRDDMethods extends RasterRDDMethods {
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than the input
    * integer, else 0.
    */
  def localLess(i: Int): RasterRDD = rasterRDD.mapTiles {
    case TmsTile(t, r) => TmsTile(t, Less(r, i))
  }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than the input
    * integer, else 0.
    */
  def <(i: Int): RasterRDD = localLess(i)
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than the input
    * integer, else 0.
    */
  def localLessRightAssociative(i: Int): RasterRDD = rasterRDD.mapTiles {
    case TmsTile(t, r) => TmsTile(t, Less(i, r))
  }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than the input
    * integer, else 0.
    *
    * @note Syntax has double '<' due to '<:' operator being reserved in Scala.
    */
  def <<:(i: Int): RasterRDD = localLessRightAssociative(i)
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than the input
    * double, else 0.
    */
  def localLess(d: Double): RasterRDD = rasterRDD.mapTiles {
    case TmsTile(t, r) => TmsTile(t, Less(r, d))
  }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than the input
    * double, else 0.
    */
  def localLessRightAssociative(d: Double): RasterRDD = rasterRDD.mapTiles {
    case TmsTile(t, r) => TmsTile(t, Less(d, r))
  }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than the input
    * double, else 0.
    */
  def <(d: Double): RasterRDD = localLess(d)
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than the input
    * double, else 0.
    *
    * @note Syntax has double '<' due to '<:' operator being reserved in Scala.
    */
  def <<:(d: Double): RasterRDD = localLessRightAssociative(d)
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell valued of the rasters are less than the next
    * raster, else 0.
    */
  def localLess(other: RasterRDD): RasterRDD = rasterRDD.combineTiles(other) {
    case (TmsTile(t1, r1), TmsTile(t2, r2)) => TmsTile(t1, Less(r1, r2))
  }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell valued of the rasters are less than the next
    * raster, else 0.
    */
  def <(other: RasterRDD): RasterRDD = localLess(other)
}
