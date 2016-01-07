package geotrellis.spark.io.index

import geotrellis.spark._
import geotrellis.spark.io.index.hilbert._

import com.github.nscala_time.time.Imports._


private[index] trait HilbertKeyIndexMethod

object HilbertKeyIndexMethod extends HilbertKeyIndexMethod {
  implicit def spatialKeyIndexIndex(m: HilbertKeyIndexMethod): KeyIndexMethod[SpatialKey, HilbertSpatialKeyIndex] =
    new KeyIndexMethod[SpatialKey, HilbertSpatialKeyIndex] {
      def createIndex(keyBounds: KeyBounds[SpatialKey]) = {
        val xResolution = resolution(keyBounds.maxKey.row - keyBounds.minKey.row)
        val yResolution = resolution(keyBounds.maxKey.col - keyBounds.minKey.col)
        HilbertSpatialKeyIndex(keyBounds, xResolution, yResolution)
      }
    }

  def apply(temporalResolution: Int): KeyIndexMethod[SpaceTimeKey, HilbertSpaceTimeKeyIndex] =
    new KeyIndexMethod[SpaceTimeKey, HilbertSpaceTimeKeyIndex] {
      def createIndex(keyBounds: KeyBounds[SpaceTimeKey]) = {
        val xResolution = resolution(keyBounds.maxKey.row - keyBounds.minKey.row)
        val yResolution = resolution(keyBounds.maxKey.col - keyBounds.minKey.col)

        HilbertSpaceTimeKeyIndex(keyBounds, xResolution, yResolution, temporalResolution)
      }
    }

  def apply(minDate: DateTime, maxDate: DateTime, temporalResolution: Int): KeyIndexMethod[SpaceTimeKey, HilbertSpaceTimeKeyIndex] =
    new KeyIndexMethod[SpaceTimeKey, HilbertSpaceTimeKeyIndex] {
      def createIndex(keyBounds: KeyBounds[SpaceTimeKey]) = {
        val adjustedKeyBounds = {
          val minKey = keyBounds.minKey
          val maxKey = keyBounds.maxKey
          KeyBounds[SpaceTimeKey](SpaceTimeKey(minKey.col, minKey.row, minDate), SpaceTimeKey(maxKey.col, maxKey.row, maxDate))
        }
        val xResolution = resolution(keyBounds.maxKey.row - keyBounds.minKey.row)
        val yResolution = resolution(keyBounds.maxKey.col - keyBounds.minKey.col)
        HilbertSpaceTimeKeyIndex(adjustedKeyBounds, xResolution, yResolution, temporalResolution)
      }
    }
}
