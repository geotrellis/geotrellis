package geotrellis

/**
 * Module utilities. Originally from geotrellis-benchmark
 */
package object bench {
  /** Sugar for building arrays using a per-cell init function */
  def init[A: Manifest](size: Int)(init: => A) = {
    val data = Array.ofDim[A](size)
    for (i <- 0 until size) data(i) = init
    data
  }
}
