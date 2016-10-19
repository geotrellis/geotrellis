package geotrellis.spark.io.avro

import java.io.ByteArrayInputStream
import java.util.zip.{InflaterInputStream, DeflaterOutputStream, Deflater}
import org.apache.avro.generic._
import org.apache.avro.io._
import org.apache.avro._
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.ByteArrayOutputStream

object AvroEncoder {
  val deflater =new Deflater(Deflater.BEST_SPEED)

  def compress(bytes: Array[Byte]): Array[Byte] = {
    val deflater = new java.util.zip.Deflater
    val baos = new ByteArrayOutputStream
    val dos = new DeflaterOutputStream(baos, deflater)
    dos.write(bytes)
    baos.close()
    dos.finish()
    dos.close()
    baos.toByteArray
  }

  def  decompress(bytes: Array[Byte]): Array[Byte] = {
    val deflater = new java.util.zip.Inflater()
    val bytesIn = new ByteArrayInputStream(bytes)
    val in = new InflaterInputStream(bytesIn, deflater)
    IOUtils.toByteArray(in)
  }

  def toBinary[T: AvroRecordCodec](thing: T): Array[Byte] = {
    val format = implicitly[AvroRecordCodec[T]]
    val schema: Schema = format.schema

    val writer = new GenericDatumWriter[GenericRecord](schema)
    val jos = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(jos, null)
    writer.write(format.encode(thing), encoder)
    encoder.flush()
    compress(jos.toByteArray)
  }

  def fromBinary[T: AvroRecordCodec](bytes: Array[Byte]): T = {
    val format = implicitly[AvroRecordCodec[T]]
    fromBinary[T](format.schema, bytes)
  }

  def fromBinary[T: AvroRecordCodec](writerSchema: Schema, bytes: Array[Byte]): T = {
    val format = implicitly[AvroRecordCodec[T]]
    val schema = format.schema

    val reader = new GenericDatumReader[GenericRecord](writerSchema, schema)
    val decoder = DecoderFactory.get().binaryDecoder(decompress(bytes), null)
    try {
      val rec = reader.read(null.asInstanceOf[GenericRecord], decoder)
      format.decode(rec)
    } catch {
      case e: AvroTypeException =>
        throw new AvroTypeException(e.getMessage + ". " +
          "This can be caused by using a type parameter which doesn't match the object being deserialized.")
    }
  }

  def toJson[T: AvroRecordCodec](thing: T): String = {
    val format = implicitly[AvroRecordCodec[T]]
    val schema = format.schema

    val writer = new GenericDatumWriter[GenericRecord](schema)
    val jos = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().jsonEncoder(schema, jos)
    writer.write(format.encode(thing), encoder)
    encoder.flush()
    jos.toByteArray.map(_.toChar).mkString
  }

  def fromJson[T: AvroRecordCodec](json: String): T = {
    val format = implicitly[AvroRecordCodec[T]]
    val schema = format.schema

    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().jsonDecoder(schema, json)
    try {
      val rec = reader.read(null.asInstanceOf[GenericRecord], decoder)
      format.decode(rec)
    } catch {
      case e: AvroTypeException =>
        throw new AvroTypeException(e.getMessage + ". " +
          "This can be caused by using a type parameter which doesn't match the object being deserialized.")
    }
  }
}
