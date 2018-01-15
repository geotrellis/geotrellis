package geotrellis.spark.pipeline.ast.singleband.temporal

import io.circe.syntax._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.pipeline.ast._
import geotrellis.spark.pipeline.json.transform
import org.apache.spark.SparkContext

case class RetileToLayout(
  node: Node[TileLayerRDD[SpaceTimeKey]],
  arg: transform.RetileToLayout
) extends Transform[TileLayerRDD[SpaceTimeKey], TileLayerRDD[SpaceTimeKey]] {
  def asJson = node.asJson :+ arg.asJson
  def eval(implicit sc: SparkContext): TileLayerRDD[SpaceTimeKey] =
    Transform.retileToLayoutTemporal(arg)(node.eval)
}
