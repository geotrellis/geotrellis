package geotrellis.spark.etl.accumulo

import geotrellis.raster.MultiBandTile
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.accumulo.AccumuloLayerWriter
import geotrellis.spark.io.index.KeyIndexMethod

import org.apache.spark.SparkContext

class MultibandSpatialAccumuloOutput extends AccumuloOutput[SpatialKey, MultiBandTile, RasterMetaData[SpatialKey]] {
  def writer(method: KeyIndexMethod[SpatialKey], props: Parameters)(implicit sc: SparkContext) =
    AccumuloLayerWriter(getInstance(props), props("table"), strategy(props)).writer[SpatialKey, MultiBandTile, RasterMetaData[SpatialKey]](method)
}
