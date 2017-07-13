package geotrellis.spark.pipeline.ast.singleband.temporal

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.pipeline.ast._
import geotrellis.spark.pipeline.json.write
import org.apache.spark.SparkContext

case class FileWrite(
  node: Node[(Int,TileLayerRDD[SpaceTimeKey])],
  arg: write.File
) extends Write[(Int, TileLayerRDD[SpaceTimeKey])] {
  def get(implicit sc: SparkContext): (Int, TileLayerRDD[SpaceTimeKey]) = arg.eval(node.get)
  def validate: (Boolean, String) = {
    val (f, msg) = if (node == null) (false, s"${this.getClass} has no node")
    else node.validation
    val (fs, msgs) = validation
    (f && fs, msgs ++ msg)
  }
}