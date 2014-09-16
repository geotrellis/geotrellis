package geotrellis.raster.multiband.op.local

import geotrellis.raster._
import geotrellis.raster.multiband._

/**
 * Computes the ndvi of given bands in a MultiBandTile.
 */

object Ndvi extends Serializable {
  /** find ndvi of given bands of a MultiBandTile. */
  def apply(m: MultiBandTile, f: Int, l: Int): Tile =
    if (f > m.bands || l > m.bands) {
      throw new IndexOutOfBoundsException("Index of bands")
    } else {
      m.getBand(f).dualCombine(m.getBand(l))((r, n) =>
        if ((n+r) == 0) NODATA
        else (n - r) / (n + r))((r, n) =>
        if ((n+r) == 0) Double.NaN
        else (n - r) / (n + r))
    }
}