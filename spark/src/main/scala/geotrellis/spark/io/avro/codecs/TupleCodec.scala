package geotrellis.spark.io.avro.codecs

import geotrellis.spark.io.avro._
import org.apache.avro._
import org.apache.avro.generic.GenericRecord

class TupleCodec[A, B](implicit a: AvroRecordCodec[A], b: AvroRecordCodec[B]) extends AvroRecordCodec[(A, B)] {
  def schema = SchemaBuilder.record("Tuple2").namespace("scala")
    .fields()
    .name("_1").`type`(a.schema).noDefault()
    .name("_2").`type`(b.schema).noDefault()
  .endRecord()

  def encode(tuple: (A, B), rec: GenericRecord) = {
    rec.put("_1", a.encode(tuple._1))
    rec.put("_2", b.encode(tuple._2))
  }

  def decode(rec: GenericRecord) =
    a.decode(rec[GenericRecord]("_1")) -> b.decode(rec[GenericRecord]("_2"))
}

object TupleCodec {
  def apply[A: AvroRecordCodec, B: AvroRecordCodec] = new TupleCodec[A, B]
}