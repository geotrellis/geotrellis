package geotrellis.spark.op.local

import geotrellis.spark._
import geotrellis.raster.op.local.LessOrEqual
import geotrellis.raster.Tile

trait LessOrEqualRasterRDDMethods[K] extends RasterRDDMethods[K] {
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input integer, else 0.
    */
  def localLessOrEqual(i: Int): RasterRDD[K, Tile] =
    rasterRDD.mapPairs { case (t, r) => (t, LessOrEqual(r, i)) }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input integer, else 0.
    */
  def localLessOrEqualRightAssociative(i: Int): RasterRDD[K, Tile] =
    rasterRDD.mapPairs { case (t, r) => (t, LessOrEqual(i, r)) }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input integer, else 0.
    */
  def <=(i: Int): RasterRDD[K, Tile] = localLessOrEqual(i)
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input integer, else 0.
    */
  def <=:(i: Int): RasterRDD[K, Tile] = localLessOrEqualRightAssociative(i)
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input double, else 0.
    */
  def localLessOrEqual(d: Double): RasterRDD[K, Tile] =
    rasterRDD.mapPairs { case (t, r) => (t, LessOrEqual(r, d)) }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input double, else 0.
    */
  def localLessOrEqualRightAssociative(d: Double): RasterRDD[K, Tile] =
    rasterRDD.mapPairs { case (t, r) => (t, LessOrEqual(d, r)) }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input double, else 0.
    */
  def <=(d: Double): RasterRDD[K, Tile] = localLessOrEqual(d)
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell value of the input raster is less than or equal to
    * the input double, else 0.
    */
  def <=:(d: Double): RasterRDD[K, Tile] = localLessOrEqualRightAssociative(d)
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell valued of the rasters are less than or equal to the
    * next raster, else 0.
    */
  def localLessOrEqual(other: RasterRDD[K, Tile]): RasterRDD[K, Tile] = rasterRDD.combineTiles(other) {
    case (t1, t2) => LessOrEqual(t1, t2)
  }
  /**
    * Returns a Tile with data of TypeBit, where cell values equal 1 if
    * the corresponding cell valued of the rasters are less than or equal to the
    * next raster, else 0.
    */
  def <=(other: RasterRDD[K, Tile]): RasterRDD[K, Tile] = localLessOrEqual(other)
}
