package geotrellis.raster

import scala.collection.mutable
import spire.syntax.cfor._

/**
  * The companion object for the [[GridBounds]] type.
  */
object GridBounds {
  /**
    * Given a [[CellGrid]], produce the corresponding [[GridBounds]].
    *
    * @param  r  The given CellGrid
    */
  def apply(r: CellGrid): GridBounds =
    GridBounds(0, 0, r.cols-1, r.rows-1)

  /**
    * Given a sequence of keys, return a [[GridBounds]] of minimal
    * size which covers them.
    *
    * @param  keys  The sequence of keys to cover
    */
  def envelope(keys: Iterable[Product2[Int, Int]]): GridBounds = {
    var colMin = Integer.MAX_VALUE
    var colMax = Integer.MIN_VALUE
    var rowMin = Integer.MAX_VALUE
    var rowMax = Integer.MIN_VALUE

    for (key <- keys) {
      val col = key._1
      val row = key._2
      if (col < colMin) colMin = col
      if (col > colMax) colMax = col
      if (row < rowMin) rowMin = row
      if (row > rowMax) rowMax = row
    }
    GridBounds(colMin, rowMin, colMax, rowMax)
  }

  /**
    * Creates a sequence of distinct [[GridBounds]] out of a set of
    * potentially overlapping grid bounds.
    *
    * @param  gridBounds  A traversable collection of GridBounds
    */
  def distinct(gridBounds: Traversable[GridBounds]): Seq[GridBounds] =
    gridBounds.foldLeft(Seq[GridBounds]()) { (acc, bounds) =>
      acc ++ acc.foldLeft(Seq(bounds)) { (cuts, bounds) =>
        cuts.flatMap(_ - bounds)
      }
    }
}

/**
  * Represents grid coordinates of a subsection of a RasterExtent.
  * These coordinates are inclusive.
  */
case class GridBounds(colMin: Int, rowMin: Int, colMax: Int, rowMax: Int) {
  def width = colMax - colMin + 1
  def height = rowMax - rowMin + 1
  if(width.toLong * height.toLong > Int.MaxValue) { sys.error(s"Cannot construct grid bounds of this size: $width x $height") }
  def size = width * height
  def isEmpty = size == 0

  /**
    * Return true if the present [[GridBounds]] contains the position
    * pointed to by the given column and row, otherwise false.
    *
    * @param  col  The column
    * @param  row  The row
    */
  def contains(col: Int, row: Int): Boolean =
    (colMin <= col && col <= colMax) &&
    (rowMin <= row && row <= rowMax)

  /**
    * Returns true if the present [[GridBounds]] and the given one
    * intersect (including their boundaries), otherwise returns false.
    *
    * @param  other  The other GridBounds
    */
  def intersects(other: GridBounds): Boolean =
    !(colMax < other.colMin || other.colMax < colMin) &&
    !(rowMax < other.rowMin || other.rowMax < rowMin)

  /**
    * Another name for the 'minus' method.
    *
    * @param  other  The other GridBounds
    */
  def -(other: GridBounds): Seq[GridBounds] = minus(other)

  /**
    * Returns the difference of the present [[GridBounds]] and the
    * given one.  This returns a sequence, because the difference may
    * consist of more than one GridBounds.
    *
    * @param  other  The other GridBounds
    */
  def minus(other: GridBounds): Seq[GridBounds] =
    if(!intersects(other)) {
      Seq(this)
    } else {
      val overlapColMin =
        if(colMin < other.colMin) other.colMin
        else colMin

      val overlapColMax =
        if(colMax < other.colMax) colMax
        else other.colMax

      val overlapRowMin =
        if(rowMin < other.rowMin) other.rowMin
        else rowMin

      val overlapRowMax =
        if(rowMax < other.rowMax) rowMax
        else other.rowMax

      val result = mutable.ListBuffer[GridBounds]()
      // Left cut
      if(colMin < overlapColMin) {
        result += GridBounds(colMin, rowMin, overlapColMin - 1, rowMax)
      }

      // Right cut
      if(overlapColMax < colMax) {
        result += GridBounds(overlapColMax + 1, rowMin, colMax, rowMax)
      }

      // Top cut
      if(rowMin < overlapRowMin) {
        result += GridBounds(overlapColMin, rowMin, overlapColMax, overlapRowMin - 1)
      }

      // Bottom cut
      if(overlapRowMax < rowMax) {
        result += GridBounds(overlapColMin, overlapRowMax + 1, overlapColMax, rowMax)
      }
      result
    }

  /**
    * Return the coordinates covered by the present [[GridBounds]].
    */
  def coords: Array[(Int, Int)] = {
    val arr = Array.ofDim[(Int, Int)](width*height)
    cfor(0)(_ < height, _ + 1) { row =>
      cfor(0)(_ < width, _ + 1) { col =>
        arr(row * width + col) =
          (col + colMin, row + rowMin)
      }
    }
    arr
  }

  /**
    * Return the intersection of the present [[GridBounds]] and the
    * given [[CellGrid]].
    *
    * @param  cellGrid  The cellGrid to intersect with
    */
  def intersection(cellGrid: CellGrid): Option[GridBounds] =
    intersection(GridBounds(cellGrid))

  /**
    * Return the intersection of the present [[GridBounds]] and the
    * given [[GridBounds]].
    *
    * @param  other  The other GridBounds
    */
  def intersection(other: GridBounds): Option[GridBounds] =
    if(!intersects(other)) {
      None
    } else {
      Some(
        GridBounds(
          math.max(colMin, other.colMin),
          math.max(rowMin, other.rowMin),
          math.min(colMax, other.colMax),
          math.min(rowMax, other.rowMax)
        )
      )
    }
}
