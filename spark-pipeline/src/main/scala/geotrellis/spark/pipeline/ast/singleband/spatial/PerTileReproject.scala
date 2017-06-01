package geotrellis.spark.pipeline.ast.singleband.spatial

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.pipeline.ast._
import geotrellis.spark.pipeline.json
import geotrellis.vector._
import org.apache.spark.rdd.RDD

case class PerTileReproject(
  node: Node[RDD[(ProjectedExtent, Tile)]],
  arg: json.TransformPerTileReproject
) extends Transform[RDD[(ProjectedExtent, Tile)], RDD[(ProjectedExtent, Tile)]] {
  def get: RDD[(ProjectedExtent, Tile)] = arg.eval(node.get)
}
