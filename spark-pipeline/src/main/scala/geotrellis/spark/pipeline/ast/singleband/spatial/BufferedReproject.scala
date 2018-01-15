package geotrellis.spark.pipeline.ast.singleband.spatial

import io.circe.syntax._

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.pipeline.ast._
import geotrellis.spark.pipeline.json.transform
import geotrellis.vector._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class BufferedReproject(
  node: Node[TileLayerRDD[SpatialKey]],
  arg: transform.Reproject
) extends Transform[TileLayerRDD[SpatialKey], TileLayerRDD[SpatialKey]] {
  def asJson = node.asJson :+ arg.asJson
  def get(implicit sc: SparkContext): TileLayerRDD[SpatialKey] = Transform.bufferedReproject(arg)(node.get)
}
