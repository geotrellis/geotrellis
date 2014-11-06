package geotrellis.spark

import geotrellis.raster.GridBounds

case class SpaceFilter[K: SpatialComponent](bounds: GridBounds) extends KeyFilter[K] {
  def includeKey(key: K): Boolean = {
    val SpatialKey(col, row) = key.spatialComponent
    bounds.contains(col, row)
  }

  def includePartition(minKey: KeyBound[K], maxKey: KeyBound[K]): Boolean = {
    val (minCol, minRow) = 
      minKey match {
        case _: MinKeyBound[K] => (0, 0)
        case _: MaxKeyBound[K] => (Int.MaxValue, Int.MaxValue)
        case ValueKeyBound(value) =>
          val SpatialKey(col, row) = value.spatialComponent
          (col, row)
      }

    val (maxCol, maxRow) =
      minKey match {
        case _: MinKeyBound[K] => (0, 0)
        case _: MaxKeyBound[K] => (Int.MaxValue, Int.MaxValue)
        case ValueKeyBound(value) =>
          val SpatialKey(col, row) = value.spatialComponent
          (col, row)
      }

    bounds.intersects(GridBounds(minCol, minRow, maxCol, maxRow))
  }
}

object SpaceFilter {
  def apply[K: SpatialComponent](col: Int, row: Int): SpaceFilter[K] =
    SpaceFilter[K](GridBounds(col, row, col, row))
}
