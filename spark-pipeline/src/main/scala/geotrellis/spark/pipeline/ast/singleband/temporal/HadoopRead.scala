package geotrellis.spark.pipeline.ast.singleband.temporal

import geotrellis.raster._
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.spark.pipeline.ast.Read
import geotrellis.spark.pipeline.json
import org.apache.spark.rdd.RDD

case class HadoopRead(read: json.ReadHadoop) extends Read[RDD[(TemporalProjectedExtent, Tile)]] {
  def get: RDD[(TemporalProjectedExtent, Tile)] = ???
}
