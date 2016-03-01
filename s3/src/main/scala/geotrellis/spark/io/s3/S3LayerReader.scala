package geotrellis.spark.io.s3

import geotrellis.raster.{MultiBandTile, Tile}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.json._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.index._
import org.apache.avro.Schema
import geotrellis.spark.utils.cache._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spray.json.{JsObject, JsonFormat}
import AttributeStore.Fields
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.reflect.ClassTag


/**
 * Handles reading raster RDDs and their metadata from S3.
 *
 * @param attributeStore  AttributeStore that contains metadata for corresponding LayerId
 * @param getCache        Optional cache function to be used when reading S3 objects.
 * @tparam K              Type of RDD Key (ex: SpatialKey)
 * @tparam V              Type of RDD Value (ex: Tile or MultiBandTile )
 * @tparam M              Type of Metadata associated with the RDD[(K,V)]
 */
class S3LayerReader[K: Boundable: JsonFormat: ClassTag, V: ClassTag, M: JsonFormat](
    val attributeStore: AttributeStore[JsonFormat],
    rddReader: S3RDDReader[K, V],
    getCache: Option[LayerId => Cache[Long, Array[Byte]]] = None)
  (implicit sc: SparkContext)
  extends FilteringLayerReader[LayerId, K, M, RDD[(K, V)] with Metadata[M]] with LazyLogging {

  val defaultNumPartitions = sc.defaultParallelism

  def read(id: LayerId, rasterQuery: RDDQuery[K, M], numPartitions: Int) = {
    if(!attributeStore.layerExists(id)) throw new LayerNotFoundError(id)

    val (header, metadata, keyBounds, keyIndex, writerSchema) = try {
      attributeStore.readLayerAttributes[S3LayerHeader, M, KeyBounds[K], KeyIndex[K], Schema](id)
    } catch {
      case e: AttributeNotFoundError => throw new LayerReadError(id).initCause(e)
    }

    val bucket = header.bucket
    val prefix = header.key

    val queryKeyBounds = rasterQuery(metadata, keyBounds)
    val maxWidth = Index.digits(keyIndex.toIndex(keyBounds.maxKey))
    val keyPath = (index: Long) => makePath(prefix, Index.encode(index, maxWidth))
    val decompose = (bounds: KeyBounds[K]) => keyIndex.indexRanges(bounds)
    val cache = getCache.map(f => f(id))
    val rdd = rddReader.read(bucket, keyPath, queryKeyBounds, decompose, Some(writerSchema), cache, numPartitions)

    new ContextRDD(rdd, metadata)
  }
}

object S3LayerReader {
  def apply[
    K: Boundable: AvroRecordCodec: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat
  ](
    attributeStore: AttributeStore[JsonFormat],
    getCache: Option[LayerId => Cache[Long, Array[Byte]]] = None
  )(implicit sc: SparkContext): S3LayerReader[K, V, M] =
    new S3LayerReader[K, V, M](
      attributeStore,
      new S3RDDReader[K, V],
      getCache
    )

  def apply[
    K: Boundable: AvroRecordCodec: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat
  ](attributeStore: AttributeStore[JsonFormat])(implicit sc: SparkContext): S3LayerReader[K, V, M] =
    apply[K, V, M](attributeStore, None)

  def apply[
    K: Boundable: AvroRecordCodec: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat
  ](bucket: String, prefix: String, getCache: Option[LayerId => Cache[Long, Array[Byte]]])(implicit sc: SparkContext): S3LayerReader[K, V, M] =
    apply[K, V, M](new S3AttributeStore(bucket, prefix), getCache)

  def apply[
    K: Boundable: AvroRecordCodec: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat
  ](bucket: String, prefix: String)(implicit sc: SparkContext): S3LayerReader[K, V, M] =
    apply[K, V, M](bucket, prefix, None)

  def spatial(bucket: String, prefix: String)(implicit sc: SparkContext): S3LayerReader[SpatialKey, Tile, RasterMetaData[SpatialKey]] =
    apply[SpatialKey, Tile, RasterMetaData[SpatialKey]](bucket, prefix)

  def spatialMultiBand(bucket: String, prefix: String)(implicit sc: SparkContext): S3LayerReader[SpatialKey, MultiBandTile, RasterMetaData[SpatialKey]] =
    apply[SpatialKey, MultiBandTile, RasterMetaData[SpatialKey]](bucket, prefix)

  def spaceTime(bucket: String, prefix: String)(implicit sc: SparkContext): S3LayerReader[SpaceTimeKey, Tile, RasterMetaData[SpaceTimeKey]] =
    apply[SpaceTimeKey, Tile, RasterMetaData[SpaceTimeKey]](bucket, prefix)


  def spaceTimeMultiBand(bucket: String, prefix: String)(implicit sc: SparkContext): S3LayerReader[SpaceTimeKey, MultiBandTile, RasterMetaData[SpaceTimeKey]] =
    apply[SpaceTimeKey, MultiBandTile, RasterMetaData[SpaceTimeKey]](bucket, prefix)
}
