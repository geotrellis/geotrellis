package geotrellis.spark.op.local

import geotrellis.spark._
import geotrellis.raster.Tile
import geotrellis.raster.op.local.Divide

trait DivideRasterRDDMethods[K] extends RasterRDDMethods[K] {
  /** Divide each value of the raster by a constant value.*/
  def localDivide(i: Int): RasterRDD[K, Tile] =
    rasterRDD.mapPairs { case (t, r) => (t, Divide(r, i)) }

  /** Divide each value of the raster by a constant value.*/
  def /(i: Int): RasterRDD[K, Tile] = localDivide(i)

  /** Divide a constant value by each cell value.*/
  def localDivideValue(i: Int): RasterRDD[K, Tile] =
    rasterRDD.mapPairs { case (t, r) => (t, Divide(i, r)) }

  /** Divide a constant value by each cell value.*/
  def /:(i: Int): RasterRDD[K, Tile] = localDivideValue(i)

  /** Divide each value of a raster by a double constant value.*/
  def localDivide(d: Double): RasterRDD[K, Tile] =
    rasterRDD.mapPairs { case (t, r) => (t, Divide(r, d)) }

  /** Divide each value of a raster by a double constant value.*/
  def /(d: Double): RasterRDD[K, Tile] = localDivide(d)

  /** Divide a double constant value by each cell value.*/
  def localDivideValue(d: Double): RasterRDD[K, Tile] =
    rasterRDD.mapPairs { case (t, r) => (t, Divide(d, r)) }

  /** Divide a double constant value by each cell value.*/
  def /:(d: Double): RasterRDD[K, Tile] = localDivideValue(d)

  /** Divide the values of each cell in each raster. */
  def localDivide(other: RasterRDD[K, Tile]): RasterRDD[K, Tile] =
    rasterRDD.combineTiles(other) { case (t1, t2) => Divide(t1, t2) }

  /** Divide the values of each cell in each raster. */
  def /(other: RasterRDD[K, Tile]): RasterRDD[K, Tile] = localDivide(other)

  def localDivide(others: Traversable[RasterRDD[K, Tile]]): RasterRDD[K, Tile] =
    rasterRDD
      .combinePairs(others.toSeq) { case tiles =>
        (tiles.head.id, Divide(tiles.map(_.tile)))
      }

  def /(others: Traversable[RasterRDD[K, Tile]]): RasterRDD[K, Tile] = localDivide(others)
}
