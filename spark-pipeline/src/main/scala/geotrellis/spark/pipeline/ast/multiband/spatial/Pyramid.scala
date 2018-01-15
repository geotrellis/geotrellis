package geotrellis.spark.pipeline.ast.multiband.spatial

import io.circe.syntax._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.pipeline.ast._
import geotrellis.spark.pipeline.json.transform
import geotrellis.vector._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class Pyramid(
  node: Node[MultibandTileLayerRDD[SpatialKey]],
  arg: transform.Pyramid
) extends Transform[MultibandTileLayerRDD[SpatialKey], Stream[(Int, MultibandTileLayerRDD[SpatialKey])]] {
  def asJson = node.asJson :+ arg.asJson
  def get(implicit sc: SparkContext): Stream[(Int, MultibandTileLayerRDD[SpatialKey])] =
    Transform.pyramid(arg)(node.get)
}
