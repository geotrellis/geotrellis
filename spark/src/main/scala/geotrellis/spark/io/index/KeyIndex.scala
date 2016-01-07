package geotrellis.spark.io.index

import geotrellis.spark._

object KeyIndexIds {
  val hilbertSpaceTimeKeyIndex = "HilbertSpaceTimeKeyIndex"
  val hilbertSpatialKeyIndex   = "HilbertSpatialKeyIndex"
  val rowMajorSpatialKeyIndex  = "RowMajorSpatialKeyIndex"
  val zSpaceTimeKeyIndex       = "ZSpaceTimeKeyIndex"
  val zSpatialKeyIndex         = "ZSpatialKeyIndex"
}

trait KeyIndex[K] extends Serializable {
  def toIndex(key: K): Long
  def indexRanges(keyRange: (K, K)): Seq[(Long, Long)]
}

trait KeyIndexMethod[K, I <: KeyIndex[K]] extends Serializable {
  /** Helper method to get the resolution of a dimension. Takes the ceiling. */
  def resolution(length: Double): Int = math.ceil(scala.math.log(length) / scala.math.log(2)).toInt

  def createIndex(keyBounds: KeyBounds[K]): I
}
