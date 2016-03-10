package geotrellis.spark.io.file

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.index._
import geotrellis.spark.io.json._
import geotrellis.spark.merge._

import com.typesafe.scalalogging.slf4j._
import org.apache.avro.Schema
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spray.json._

import java.io.File
import scala.reflect._

class FileLayerUpdater(
  catalogPath: String,
  attributeStore: AttributeStore[JsonFormat],
  layerReader: FileLayerReader
) extends LayerUpdater[LayerId] with LazyLogging {

  protected def _update[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: Component[?, Bounds[K]]: Mergable
  ](id: LayerId, rdd: RDD[(K, V)] with Metadata[M], keyBounds: KeyBounds[K], mergeFunc: (V, V) => V) = {
    if (!attributeStore.layerExists(id)) throw new LayerNotFoundError(id)
    val (header, metadata, keyIndex, writerSchema) = try {
      attributeStore.readLayerAttributes[FileLayerHeader, M, KeyIndex[K], Schema](id)
    } catch {
      case e: AttributeNotFoundError => throw new LayerUpdateError(id).initCause(e)
    }

    val path = header.path

    if (!(keyIndex.keyBounds contains keyBounds))
      throw new LayerOutOfKeyBoundsError(id, keyIndex.keyBounds)

    val maxWidth = Index.digits(keyIndex.toIndex(keyIndex.keyBounds.maxKey))
    val keyPath = KeyPathGenerator(catalogPath, path, keyIndex, maxWidth)
    val layerPath = new File(catalogPath, path).getAbsolutePath

    logger.info(s"Saving updated RDD for layer ${id} to $path")
    val existingTiles =
      if(schemaHasChanged[K, V](writerSchema)) {
        logger.warn(s"RDD schema has changed, this requires rewriting the entire layer.")
        layerReader
          .read[K, V, M](id)

      } else {
        val query =
          new RDDQuery[K, M]
            .where(Intersects(rdd.metadata.getComponent[Bounds[K]].get))

        layerReader.read[K, V, M](id, query, layerReader.defaultNumPartitions, filterIndexOnly = true)
      }

    val updatedMetadata: M =
      metadata.merge(rdd.metadata)

    val updatedRdd: RDD[(K, V)] =
      existingTiles
        .leftOuterJoin(rdd)
        .mapValues { case (layerTile, updateTile) =>
          updateTile match {
            case Some(tile) =>
              mergeFunc(layerTile, tile)
            case None =>
              layerTile
          }
      }

    val codec  = KeyValueRecordCodec[K, V]
    val schema = codec.schema

    // Write updated metadata, and the possibly updated schema
    // Only really need to write the metadata and schema
    attributeStore.writeLayerAttributes[FileLayerHeader, M, KeyIndex[K], Schema](id, header, updatedMetadata, keyIndex, schema)
    FileRDDWriter.write[K, V](updatedRdd, layerPath, keyPath)
  }
}

object FileLayerUpdater {
  def apply(catalogPath: String)(implicit sc: SparkContext): FileLayerUpdater =
    new FileLayerUpdater(
      catalogPath,
      FileAttributeStore(catalogPath),
      FileLayerReader(catalogPath)
    )
}
