package geotrellis.spark.pipeline.ast.singleband.spatial

import io.circe.syntax._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.pipeline.ast._
import geotrellis.spark.pipeline.json.transform
import geotrellis.vector.ProjectedExtent

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class Pyramid(
  node: Node[TileLayerRDD[SpatialKey]],
  arg: transform.Pyramid
) extends Transform[TileLayerRDD[SpatialKey], Stream[(Int, TileLayerRDD[SpatialKey])]] {
  def asJson = node.asJson :+ arg.asJson
  def get(implicit sc: SparkContext): Stream[(Int, TileLayerRDD[SpatialKey])] =
    Transform.pyramid(arg)(node.get)
  def validate: (Boolean, String) = {
    val (f, msg) = if (node == null) (false, s"${this.getClass} has no node")
    else node.validation
    val (fs, msgs) = validation
    (f && fs, msgs ++ msg)
  }
}
