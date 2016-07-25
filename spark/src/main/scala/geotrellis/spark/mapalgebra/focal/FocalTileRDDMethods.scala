package geotrellis.spark.mapalgebra.focal

import geotrellis.spark._
import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal._
import geotrellis.util.MethodExtensions


trait FocalTileLayerRDDMethods[K] extends FocalOperation[K] {

  def focalSum(n: Neighborhood, target: TargetCell = TargetCell.All) =
    focal(n) { (tile, bounds) => Sum(tile, n, bounds, target) }

  def focalMin(n: Neighborhood, target: TargetCell = TargetCell.All) =
    focal(n) { (tile, bounds) => Min(tile, n, bounds, target) }

  def focalMax(n: Neighborhood, target: TargetCell = TargetCell.All) =
    focal(n) { (tile, bounds) => Max(tile, n, bounds, target) }

  def focalMean(n: Neighborhood, target: TargetCell = TargetCell.All) =
    focal(n) { (tile, bounds) => Mean(tile, n, bounds, target) }

  def focalMedian(n: Neighborhood, target: TargetCell = TargetCell.All) =
    focal(n) { (tile, bounds) => Median(tile, n, bounds, target) }

  def focalMode(n: Neighborhood, target: TargetCell = TargetCell.All) =
    focal(n) { (tile, bounds) => Mode(tile, n, bounds, target) }

  def focalStandardDeviation(n: Neighborhood, target: TargetCell = TargetCell.All) =
    focal(n) { (tile, bounds) => StandardDeviation(tile, n, bounds, target) }

  def focalConway() = {
    val n = Square(1)
    focal(n) { (tile, bounds) => Sum(tile, n, bounds, TargetCell.All) }
  }

  def focalConvolve(k: Kernel, target: TargetCell = TargetCell.All) =
   focal(k) { (tile, bounds) => Convolve(tile, k, bounds, target) }

  def aspect() = {
    val n = Square(1)
    focalWithCellSize(n) { (tile, bounds, cellSize) =>
      Aspect(tile, n, bounds, cellSize)
    }
  }

  def slope(zFactor: Double = 1.0) = {
    val n = Square(1)
    focalWithCellSize(n) { (tile, bounds, cellSize) =>
      Slope(tile, n, bounds, cellSize, zFactor)
    }
  }
}
