package geotrellis.spark.io.accumulo

import geotrellis.raster.{MultiBandTile, Tile}
import geotrellis.spark.io.json._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark._
import geotrellis.spark.io.index.{KeyIndex, KeyIndexMethod}
import geotrellis.spark.io._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spray.json._
import scala.reflect._

class AccumuloLayerWriter[
  K: Boundable: JsonFormat: ClassTag, V: ClassTag,
  M: JsonFormat, I <: KeyIndex[K]: JsonFormat
](val attributeStore: AttributeStore[JsonFormat],
  rddWriter: BaseAccumuloRDDWriter[K, V],
  keyIndexMethod: KeyIndexMethod[K, I],
  table: String) extends Writer[LayerId, RDD[(K, V)] with Metadata[M]] {

  def write(id: LayerId, rdd: RDD[(K, V)] with Metadata[M]): Unit = {
    val header =
      AccumuloLayerHeader(
        keyClass = classTag[K].toString(),
        valueClass = classTag[V].toString(),
        tileTable = table
      )
    val metaData = rdd.metadata
    val keyBounds = implicitly[Boundable[K]].getKeyBounds(rdd)
    val keyIndex = keyIndexMethod.createIndex(keyBounds)
    val getRowId = (key: K) => index2RowId(keyIndex.toIndex(key))

    try {
      attributeStore.writeLayerAttributes(id, header, metaData, keyBounds, keyIndex, rddWriter.schema)
      rddWriter.write(rdd, table, columnFamily(id), getRowId, oneToOne = false)
    } catch {
      case e: Exception => throw new LayerWriteError(id).initCause(e)
    }
  }
}

object AccumuloLayerWriter {
  def defaultAccumuloWriteStrategy = HdfsWriteStrategy("/geotrellis-ingest")

  def apply[
    K: Boundable: AvroRecordCodec: JsonFormat: ClassTag, V: AvroRecordCodec: ClassTag,
    M: JsonFormat, I <: KeyIndex[K]: JsonFormat](
    instance: AccumuloInstance,
    table: String,
    indexMethod: KeyIndexMethod[K, I],
    strategy: AccumuloWriteStrategy = defaultAccumuloWriteStrategy
  ): AccumuloLayerWriter[K, V, M, I] =
    new AccumuloLayerWriter[K, V, M, I](
      attributeStore = AccumuloAttributeStore(instance.connector),
      rddWriter = new AccumuloRDDWriter[K, V](instance, strategy),
      keyIndexMethod = indexMethod,
      table = table
    )

  def spatial[I <: KeyIndex[SpatialKey]: JsonFormat](
    instance: AccumuloInstance,
    table: String,
    keyIndexMethod: KeyIndexMethod[SpatialKey, I],
    strategy: AccumuloWriteStrategy = defaultAccumuloWriteStrategy
  )(implicit sc: SparkContext) =
    apply[SpatialKey, Tile, RasterMetaData, I](instance, table, keyIndexMethod, strategy)

  def spatialMultiBand[I <: KeyIndex[SpatialKey]: JsonFormat](
    instance: AccumuloInstance,
    table: String,
    keyIndexMethod: KeyIndexMethod[SpatialKey, I],
    strategy: AccumuloWriteStrategy = defaultAccumuloWriteStrategy
  )(implicit sc: SparkContext) =
    apply[SpatialKey, MultiBandTile, RasterMetaData, I](instance, table, keyIndexMethod, strategy)

  def spaceTime[I <: KeyIndex[SpaceTimeKey]: JsonFormat](
    instance: AccumuloInstance,
    table: String,
    keyIndexMethod: KeyIndexMethod[SpaceTimeKey, I],
    strategy: AccumuloWriteStrategy = defaultAccumuloWriteStrategy
  )(implicit sc: SparkContext) =
    apply[SpaceTimeKey, Tile, RasterMetaData, I](instance, table, keyIndexMethod, strategy)

  def spaceTimeMultiBand[I <: KeyIndex[SpaceTimeKey]: JsonFormat](
    instance: AccumuloInstance,
    table: String,
    keyIndexMethod: KeyIndexMethod[SpaceTimeKey, I],
    strategy: AccumuloWriteStrategy = defaultAccumuloWriteStrategy
  )(implicit sc: SparkContext) =
    apply[SpaceTimeKey, MultiBandTile, RasterMetaData, I](instance, table, keyIndexMethod, strategy)
}
