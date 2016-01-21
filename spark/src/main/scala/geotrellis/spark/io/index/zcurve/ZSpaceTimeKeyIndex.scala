package geotrellis.spark.io.index.zcurve

import geotrellis.spark._
import geotrellis.spark.io.index.KeyIndex

object ZSpaceTimeKeyIndex {
  val MILLIS_PER_SECOND = 1000l
  val MILLIS_PER_MINUTE = MILLIS_PER_SECOND * 60
  val MILLIS_PER_HOUR   = MILLIS_PER_MINUTE * 60
  val MILLIS_PER_DAY    = MILLIS_PER_HOUR * 24
  val MILLIS_PER_MONTH  = MILLIS_PER_DAY * 30
  val MILLIS_PER_YEAR   = MILLIS_PER_DAY * 365

  def byYear(): ZSpaceTimeKeyIndex = byMillisecondResolution(MILLIS_PER_YEAR)

  def byMonth(): ZSpaceTimeKeyIndex = byMillisecondResolution(MILLIS_PER_MONTH)

  def byDay(): ZSpaceTimeKeyIndex = byMillisecondResolution(MILLIS_PER_DAY)

  def byHour(): ZSpaceTimeKeyIndex = byMillisecondResolution(MILLIS_PER_HOUR)

  def byMinute(): ZSpaceTimeKeyIndex = byMillisecondResolution(MILLIS_PER_MINUTE)

  def bySecond(): ZSpaceTimeKeyIndex = byMillisecondResolution(MILLIS_PER_SECOND)

  def byMillisecond(): ZSpaceTimeKeyIndex = byMillisecondResolution()

  def byMillisecondResolution(millis: Long = 1): ZSpaceTimeKeyIndex =
    new ZSpaceTimeKeyIndex(millis)
}

class ZSpaceTimeKeyIndex(val temporalResolution: Long) extends KeyIndex[SpaceTimeKey] {
  private def toZ(key: SpaceTimeKey): Z3 = Z3(key.col, key.row, (key.instant / temporalResolution).toInt)

  def toIndex(key: SpaceTimeKey): Long = toZ(key).z

  def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(Long, Long)] =
    Z3.zranges(toZ(keyRange._1), toZ(keyRange._2))
}
