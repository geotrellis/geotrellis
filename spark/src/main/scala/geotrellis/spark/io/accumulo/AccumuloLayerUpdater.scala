package geotrellis.spark.io.accumulo

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.io.index.KeyIndex
import geotrellis.spark.io.json._
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import spray.json._
import scala.reflect._

class AccumuloLayerUpdater[K: Boundable: JsonFormat: ClassTag, V: ClassTag, Container](
    val attributeStore: AttributeStore[JsonFormat],
    rddWriter: BaseAccumuloRDDWriter[K, V])
  (implicit val cons: ContainerConstructor[K, V, Container])
  extends LayerUpdater[LayerId, K, V, Container with RDD[(K, V)]] {

  def update(id: LayerId, rdd: Container with RDD[(K, V)]) = {
    if (!attributeStore.layerExists(id)) throw new LayerNotFoundError(id)
    implicit val sc = rdd.sparkContext
    implicit val mdFormat = cons.metaDataFormat

    val (existingHeader, _, existingKeyBounds, existingKeyIndex, _) = try {
      attributeStore.readLayerAttributes[AccumuloLayerHeader, cons.MetaDataType, KeyBounds[K], KeyIndex[K], Schema](id)
    } catch {
      case e: AttributeNotFoundError => throw new LayerUpdateError(id, ": unable to read metadata").initCause(e)
    }

    val boundable = implicitly[Boundable[K]]
    val keyBounds = try {
      boundable.getKeyBounds(rdd)
    } catch {
      case e: UnsupportedOperationException => throw new LayerUpdateError(id, ": empty rdd update").initCause(e)
    }

    if (!boundable.includes(keyBounds.minKey, existingKeyBounds) || !boundable.includes(keyBounds.maxKey, existingKeyBounds))
      throw new LayerOutOfKeyBoundsError(id)

    val getRowId = (key: K) => index2RowId(existingKeyIndex.toIndex(key))

    try {
      rddWriter.write(rdd, existingHeader.tileTable, columnFamily(id), getRowId, oneToOne = false)
    } catch {
      case e: Exception => throw new LayerWriteError(id).initCause(e)
    }
  }
}

object AccumuloLayerUpdater {
  def defaultAccumuloWriteStrategy = HdfsWriteStrategy("/geotrellis-ingest")

  def apply[K: SpatialComponent: Boundable: AvroRecordCodec: JsonFormat: ClassTag,
  V: AvroRecordCodec: ClassTag, Container[_]]
  (instance: AccumuloInstance,
   strategy: AccumuloWriteStrategy = defaultAccumuloWriteStrategy)
  (implicit cons: ContainerConstructor[K, V, Container[K]]): AccumuloLayerUpdater[K, V, Container[K]] =
    new AccumuloLayerUpdater(
      attributeStore = AccumuloAttributeStore(instance.connector),
      rddWriter = new AccumuloRDDWriter[K, V](instance, strategy)
    )
}
