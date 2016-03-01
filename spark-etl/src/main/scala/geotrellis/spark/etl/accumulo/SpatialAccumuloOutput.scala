package geotrellis.spark.etl.accumulo

import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.io.accumulo.AccumuloLayerWriter
import geotrellis.spark.io.index.KeyIndexMethod
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.json._

class SpatialAccumuloOutput extends AccumuloOutput[SpatialKey, Tile, RasterMetaData[SpatialKey]] {
  def writer(method: KeyIndexMethod[SpatialKey], props: Parameters) =
    AccumuloLayerWriter[SpatialKey, Tile, RasterMetaData[SpatialKey]](getInstance(props),  props("table"), method)
}
