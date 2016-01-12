package geotrellis.spark.io.file

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro.codecs.KeyValueRecordCodec
import geotrellis.spark.io.index._
import geotrellis.spark.io.json._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._
import geotrellis.raster.io.Filesystem

import org.apache.avro.Schema
import spray.json._
import spray.json.DefaultJsonProtocol._

import java.io.File
import scala.reflect.ClassTag

class FileTileReader[K: AvroRecordCodec: JsonFormat: ClassTag, V: AvroRecordCodec](
  val attributeStore: AttributeStore[JsonFormat],
  catalogPath: String
)  extends Reader[LayerId, Reader[K, V]] {

  def read(layerId: LayerId): Reader[K, V] = new Reader[K, V] {

    val (layerMetaData, _, keyBounds, keyIndex, writerSchema) =
      attributeStore.readLayerAttributes[FileLayerHeader, Unit, KeyBounds[K], KeyIndex[K], Schema](layerId)

    val maxWidth = Index.digits(keyIndex.toIndex(keyBounds.maxKey))
    val keyPath = KeyPathGenerator(catalogPath, layerMetaData.path, keyIndex, maxWidth)

    def read(key: K): V = {
      val path = keyPath(key)

      if(!new File(path).exists)
        throw new TileNotFoundError(key, layerId)

      val bytes = Filesystem.slurp(path)
      val recs = AvroEncoder.fromBinary(bytes)(KeyValueRecordCodec[K, V])

      recs
        .find { row => row._1 == key }
        .map { row => row._2 }
        .getOrElse(throw new TileNotFoundError(key, layerId))
    }
  }
}

object FileTileReader {
  def apply[K: AvroRecordCodec: JsonFormat: ClassTag, V: AvroRecordCodec](catalogPath: String): FileTileReader[K, V] =
    new FileTileReader[K, V](new FileAttributeStore(catalogPath), catalogPath)

  def apply[K: AvroRecordCodec: JsonFormat: ClassTag, V: AvroRecordCodec](attributeStore: FileAttributeStore): FileTileReader[K, V] =
    new FileTileReader[K, V](attributeStore, attributeStore.catalogPath)
}
