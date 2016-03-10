package geotrellis.spark.io.hadoop

import geotrellis.spark._
import geotrellis.spark.io._

import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import spray.json.JsonFormat
import spray.json.DefaultJsonProtocol._

object HadoopLayerReindexer {
  def apply(rootPath: Path)(implicit sc: SparkContext): LayerReindexer[LayerId] =
    GenericLayerReindexer[HadoopLayerHeader](
      attributeStore = HadoopAttributeStore(rootPath),
      layerReader    = HadoopLayerReader(rootPath),
      layerWriter    = HadoopLayerWriter(rootPath),
      layerDeleter   = HadoopLayerDeleter(rootPath),
      layerCopier    = HadoopLayerCopier(rootPath)
    )

  def apply(attributeStore: HadoopAttributeStore)(implicit sc: SparkContext): LayerReindexer[LayerId] =
    apply(attributeStore.rootPath)
}
