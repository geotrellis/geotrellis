package geotrellis.statistics

import math.{abs, ceil, min, max, sqrt}

object CompressedArrayHistogram {
  def apply(size:Int) = {
    val counts = Array.ofDim[Int](size)
    new CompressedArrayHistogram(counts, 0, Int.MinValue, Int.MaxValue)
  }

  def apply(vmin:Int, vmax:Int) = {
    val counts = Array.ofDim[Int](vmax + 1 - vmin)
    new CompressedArrayHistogram(counts, 0, vmin, vmax)
  }

  def apply(vmin:Int, vmax:Int, size:Int) = {
    val counts = Array.ofDim[Int](size)
    new CompressedArrayHistogram(counts, 0, vmin, vmax)
  }
}

// TODO: can currently only handle non-negative integers

/**
  * Data object representing a histogram using an array for internal storage,
  * which requires an initial minimum and maximum val and a specified number of 'breaks' which
  * are used to group values together into ranges.
  */
class CompressedArrayHistogram(counts:Array[Int], total:Int,
                               vmin:Int, vmax:Int) extends ArrayHistogram(counts, total) {
  val divisor = ceil((vmax - vmin).toFloat / counts.length).toInt

  //@inline
  final def compress(i:Int) = {
    val j = if (i >= vmax) { vmax - 1 } else if (i <= vmin) { vmin } else { i }
    (j - vmin) / divisor
  }

  //@inline
  final def decompress(i:Int) = (i * divisor) + vmin

  override def copy = new CompressedArrayHistogram(counts.clone, getTotalCount, vmin, vmax)

  override def getValues = {
    val zmin = getMinValue
    val zmax = getMaxValue
    (zmin until zmax).filter(getItemCount(_) > 0).toArray
  }

  override def setItem(i:Int, count:Int) { super.setItem(compress(i), count) }
  override def countItem(i:Int, count:Int=1) { super.countItem(compress(i), count) }
  override def uncountItem(i:Int) { super.uncountItem(compress(i)) }

  override def getItemCount(i:Int) = {
    // once we compress, all values become equal to the maximum value of their
    // range, e.g. [0-9] becomes 9; this means that if our inputs are:
    // [0,1,4,6,9] the count for 9 is 5 and for all others is 0.
    if ((i + 1) % divisor == 0) {
      counts(compress(i))
    } else {
      0
    }
  }

  override def getMinValue:Int = {
    val x = super.getMinValue
    if (x == Int.MaxValue) x else decompress(x)
  }

  override def getMaxValue:Int = {
    val x = super.getMaxValue
    if (x == Int.MinValue) x else decompress(x)
  }
}
