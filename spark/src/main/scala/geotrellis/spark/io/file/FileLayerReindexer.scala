package geotrellis.spark.io.file

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.index._
import geotrellis.spark.io.json._
import geotrellis.raster.io.Filesystem
import AttributeStore.Fields

import org.apache.spark.SparkContext
import spray.json.JsonFormat
import org.apache.avro.Schema

import scala.reflect.ClassTag
import java.io.File

object FileLayerReindexer {
  def apply[
    K: AvroRecordCodec: JsonFormat: Boundable: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat
  ](
    attributeStore: FileAttributeStore
  )(implicit sc: SparkContext): LayerReindexer[LayerId, K] = {
    val layerReader  = FileLayerReader[K, V, M](attributeStore)
    val layerDeleter = FileLayerDeleter(attributeStore)
    val layerWriter  = FileLayerWriter[K, V, M](attributeStore)

    val layerCopier  = new SparkLayerCopier[FileLayerHeader, K, V, M](
      attributeStore = attributeStore,
      layerReader    = layerReader,
      layerWriter    = layerWriter
    ) {
      def headerUpdate(layerId: LayerId, header: FileLayerHeader): FileLayerHeader =
        header.copy(path = LayerPath(layerId))
    }

    val layerMover = GenericLayerMover(layerCopier, layerDeleter)

    GenericLayerReindexer(layerDeleter, layerCopier, layerMover)
  }

  def apply[
    K: AvroRecordCodec: JsonFormat: Boundable: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat
  ](catalogPath: String)(implicit sc: SparkContext): LayerReindexer[LayerId, K] = {
    val attributeStore = FileAttributeStore(catalogPath)
    apply[K, V, M](attributeStore)
  }
}
