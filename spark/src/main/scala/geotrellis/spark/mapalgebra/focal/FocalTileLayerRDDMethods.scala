package geotrellis.spark.mapalgebra.focal

import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal._

trait FocalTileLayerRDDMethods[K] extends FocalOperation[K] {

  def focalSum(n: Neighborhood) = focal(n) { (tile, bounds) => Sum(tile, n, bounds) }
  def focalMin(n: Neighborhood) = focal(n) { (tile, bounds) => Min(tile, n, bounds) }
  def focalMax(n: Neighborhood) = focal(n) { (tile, bounds) => Max(tile, n, bounds) }
  def focalMean(n: Neighborhood) = focal(n) { (tile, bounds) => Mean(tile, n, bounds) }
  def focalMedian(n: Neighborhood) = focal(n) { (tile, bounds) => Median(tile, n, bounds) }
  def focalMode(n: Neighborhood) = focal(n) { (tile, bounds) => Mode(tile, n, bounds) }
  def focalStandardDeviation(n: Neighborhood) = focal(n) { (tile, bounds) => StandardDeviation(tile, n, bounds) }
  def focalConway() = { val n = Square(1) ; focal(n) { (tile, bounds) => Sum(tile, n, bounds) } }
  def focalConvolve(k: Kernel) = { focal(k) { (tile, bounds) => Convolve(tile, k, bounds) } }

  /** Calculates the aspect of each cell in a raster.
   *
   * @see [[geotrellis.raster.mapalgebra.focal.Aspect]]
   */
  def aspect() = {
    val n = Square(1)
    focalWithCellSize(n) { (tile, bounds, cellSize) =>
      Aspect(tile, n, bounds, cellSize)
    }.mapContext(_.copy(cellType = DoubleConstantNoDataCellType))
  }

  /** Calculates the slope of each cell in a raster.
   *
   * @see [[geotrellis.raster.mapalgebra.focal.Slope]]
   */
  def slope(zFactor: Double = 1.0) = {
    val n = Square(1)
    focalWithCellSize(n) { (tile, bounds, cellSize) =>
      Slope(tile, n, bounds, cellSize, zFactor)
    }.mapContext(_.copy(cellType = DoubleConstantNoDataCellType))
  }
}
