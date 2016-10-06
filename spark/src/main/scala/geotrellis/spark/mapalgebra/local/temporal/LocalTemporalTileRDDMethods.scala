package geotrellis.spark.mapalgebra.local.temporal

import geotrellis.raster._
import geotrellis.spark._
import org.apache.spark.Partitioner
import geotrellis.util.MethodExtensions

import org.apache.spark.rdd.RDD
import java.time.ZonedDateTime

import scala.reflect.ClassTag

abstract class LocalTemporalTileRDDMethods[K: ClassTag: SpatialComponent: TemporalComponent](val self: RDD[(K, Tile)])
    extends MethodExtensions[RDD[(K, Tile)]] {

  def temporalMin(
    windowSize: Int,
    unit: Int,
    start: ZonedDateTime,
    end: ZonedDateTime,
    partitioner: Option[Partitioner] = None
  ): RDD[(K, Tile)] =
    LocalTemporalStatistics.temporalMin(self, windowSize, unit, start, end, partitioner)

  def temporalMax(
    windowSize: Int,
    unit: Int,
    start: ZonedDateTime,
    end: ZonedDateTime,
    partitioner: Option[Partitioner] = None
  ): RDD[(K, Tile)] =
    LocalTemporalStatistics.temporalMax(self, windowSize, unit, start, end, partitioner)

  def temporalMean(
    windowSize: Int,
    unit: Int,
    start: ZonedDateTime,
    end: ZonedDateTime,
    partitioner: Option[Partitioner] = None
  ): RDD[(K, Tile)] =
    LocalTemporalStatistics.temporalMean(self, windowSize, unit, start, end, partitioner)

  def temporalVariance(
    windowSize: Int,
    unit: Int,
    start: ZonedDateTime,
    end: ZonedDateTime,
    partitioner: Option[Partitioner] = None
  ): RDD[(K, Tile)] =
    LocalTemporalStatistics.temporalVariance(self, windowSize, unit, start, end, partitioner)
}
