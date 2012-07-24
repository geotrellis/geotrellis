package geotrellis.raster.op.local

import geotrellis._
import geotrellis._

/**
 * Perform a function on every cell in a raster with the values from another raster.
 *
 * For example,
 * <pre>
 * val A = LoadFile(a)
 * val B = LoadFile(b)
 * 
 * // Generate a raster by adding the values of each cell in A and B 
 * val D = BinaryDoCell(R, (a, b) => a + b )
 * </pre>
 */
case class BinaryDoCell(r1:Op[Raster], r2:Op[Raster], f:(Int, Int) => Int) extends BinaryLocal {
  def handle(z1:Int, z2:Int) = f(z1, z2)
  def handleDouble(z1:Double, z2:Double) = f(RasterUtil.d2i(z1), RasterUtil.d2i(z2))
}
