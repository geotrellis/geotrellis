package geotrellis.spark.pipeline.ast.multiband.temporal

import io.circe.syntax._

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.pipeline.ast._
import geotrellis.spark.pipeline.json.write

import org.apache.spark.SparkContext

case class FileWrite(
  node: Node[Stream[(Int, MultibandTileLayerRDD[SpaceTimeKey])]],
  arg: write.JsonWrite
) extends Write[Stream[(Int, MultibandTileLayerRDD[SpaceTimeKey])]] {
  def asJson = node.asJson :+ arg.asJson
  def get(implicit sc: SparkContext): Stream[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = Write.eval(arg)(node.get)
}