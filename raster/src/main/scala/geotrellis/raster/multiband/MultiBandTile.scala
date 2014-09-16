package geotrellis.raster.multiband

import geotrellis.vector.Extent
import spire.syntax.cfor._
import geotrellis.raster.ArrayTile
import geotrellis.raster.CellType
import geotrellis.raster.RasterExtent
import geotrellis.raster.Tile
import geotrellis.raster._

object MultiBandTile {
  def apply(arr: Array[Tile]): MultiBandTile =
    MultiBandArrayTile(arr)

  def empty(cellType: CellType, noOfBands: Int, cols: Int, rows: Int): MultiBandTile =
    if (noOfBands < 2) {
      sys.error("There should be at least two Tiles to be MultiBandTile")
    } else {
      val output = Array.ofDim[Tile](noOfBands)
      cfor(0)(_ < noOfBands, _ + 1) { band =>
        output(band) = ArrayTile.empty(cellType, cols, rows)
      }
      MultiBandTile(output)
    }
}

/**
 * Base trait for a MultiBandTile.
 */
trait MultiBandTile {

  val cols: Int
  val rows: Int
  val bands: Int
  lazy val dimensions: (Int, Int) = (cols, rows)
  lazy val sizeOfBand = cols * rows

  val cellType: CellType

  def getBand(bandNo: Int): Tile

  def map(f: Int => Int): MultiBandTile
  def mapDouble(f: Double => Double): MultiBandTile

  def dualMap(f: Int => Int)(g: Double => Double): MultiBandTile =
    if (cellType.isFloatingPoint) mapDouble(g)
    else map(f)

  def convert(cellType: CellType): MultiBandTile

  /**
   * combine two multibandtiles according to given function
   */
  def combine(other: MultiBandTile)(f: (Int, Int) => Int): MultiBandTile
  def combineDouble(other: MultiBandTile)(f: (Double, Double) => Double): MultiBandTile

  def dualCombine(other: MultiBandTile)(f: (Int, Int) => Int)(g: (Double, Double) => Double): MultiBandTile =
    if (cellType.isFloatingPoint) combineDouble(other)(g)
    else combine(other)(f)

  /**
   *  combine bands in a single multibandtile according to given function
   */
  def combine(first: Int, last: Int)(f: (Int, Int) => Int): Tile
  def combineDouble(first: Int, last: Int)(f: (Double, Double) => Double): Tile

  def dualCombine(first: Int, last: Int)(f: (Int, Int) => Int)(g: (Double, Double) => Double): Tile =
    if (cellType.isFloatingPoint) combineDouble(first, last)(g)
    else combine(first, last)(f)

  def mapIfSet(f: Int => Int): MultiBandTile =
    map { i =>
      if (isNoData(i)) i
      else f(i)
    }

  def mapIfSetDouble(f: Double => Double): MultiBandTile =
    mapDouble { d =>
      if (isNoData(d)) d
      else f(d)
    }

  def dualMapIfSet(f: Int => Int)(g: Double => Double): MultiBandTile =
    if (cellType.isFloatingPoint) mapIfSetDouble(g)
    else mapIfSet(f)

  def warp(source: Extent, target: RasterExtent): MultiBandTile
  def warp(source: Extent, target: Extent): MultiBandTile
  def warp(source: Extent, targetCols: Int, targetRows: Int): MultiBandTile

}
