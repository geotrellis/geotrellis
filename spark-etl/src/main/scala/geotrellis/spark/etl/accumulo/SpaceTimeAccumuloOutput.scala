package geotrellis.spark.etl.accumulo

import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.accumulo.AccumuloLayerWriter
import geotrellis.spark.io.index.KeyIndexMethod

import org.apache.spark.SparkContext

class SpaceTimeAccumuloOutput extends AccumuloOutput[SpaceTimeKey, Tile, RasterMetaData[SpaceTimeKey]] {
  def writer(method: KeyIndexMethod[SpaceTimeKey], props: Parameters)(implicit sc: SparkContext) =
    AccumuloLayerWriter(getInstance(props), props("table"), strategy(props)).writer[SpaceTimeKey, Tile, RasterMetaData[SpaceTimeKey]](method)
}
