package geotrellis.spark.pipeline.eval.ast.multiband.spatial

import io.circe.syntax._

import geotrellis.spark._
import geotrellis.spark.pipeline.eval.ast._
import geotrellis.spark.pipeline.json.write

case class FileWrite(
  node: Node[MultibandTileLayerRDD[SpatialKey] => (Int, MultibandTileLayerRDD[SpatialKey])],
  arg: write.File
) extends Write[(Int, MultibandTileLayerRDD[SpatialKey])] {
  def asJson = node.asJson :+ arg.asJson
}