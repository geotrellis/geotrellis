package geotrellis.spark.io.accumulo

import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._
import org.apache.avro.Schema

import org.apache.hadoop.io.Text

import org.apache.spark.rdd.RDD

import org.apache.accumulo.core.data.{Key, Value}

import scala.collection.JavaConversions._

object AccumuloRDDWriter {

  def write[K: AvroRecordCodec, V: AvroRecordCodec](
    raster: RDD[(K, V)],
    instance: AccumuloInstance,
    encodeKey: K => Key,
    writeStrategy: AccumuloWriteStrategy,
    table: String
  ): Unit = {
    implicit val sc = raster.sparkContext

    val codec  = KeyValueRecordCodec[K, V]
    val schema = codec.schema

    instance.ensureTableExists(table)

    val grouped: RDD[(Key, Iterable[(K, V)])] =
      if(writeStrategy.requiresSort) {
        // Map and sort first, so that partitioner is carried over to groupByKey
        raster
          .map { case (key, value) => (encodeKey(key), (key, value)) }
          .sortByKey()
          .groupByKey()
      } else {
        // Call groupBy with numPartitions; if called without that argument or a partitioner,
        // groupBy will reuse the partitioner on the parent RDD if it is set, which could be typed
        // on a key type that may no longer by valid for the key type of the resulting RDD.
        raster
          .groupBy({ row => encodeKey(row._1) }, numPartitions = raster.partitions.length)
      }

    val kvPairs: RDD[(Key, Value)] =
      grouped
        .map { case (key, pairs) =>
          (key, new Value(AvroEncoder.toBinary(pairs.toVector)(codec)))
        }

    writeStrategy.write(kvPairs, instance, table)
  }
}
