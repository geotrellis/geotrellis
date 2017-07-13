package geotrellis.spark.pipeline.ast.singleband.spatial

import geotrellis.raster._
import geotrellis.spark.pipeline.ast._
import geotrellis.spark.pipeline.json.transform
import geotrellis.vector._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class PerTileReproject(
  node: Node[RDD[(ProjectedExtent, Tile)]],
  arg: transform.PerTileReproject
) extends Transform[RDD[(ProjectedExtent, Tile)], RDD[(ProjectedExtent, Tile)]] {
  def get(implicit sc: SparkContext): RDD[(ProjectedExtent, Tile)] = arg.eval(node.get)
  def validate: (Boolean, String) = {
    val (f, msg) = if (node == null) (false, s"${this.getClass} has no node")
    else node.validation
    val (fs, msgs) = validation
    (f && fs, msgs ++ msg)
  }
}
