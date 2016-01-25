package geotrellis.spark.io.avro.codecs

import java.nio.ByteBuffer

import geotrellis.raster._
import geotrellis.spark.io.avro._
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic._

import scala.collection.JavaConverters._

trait TileCodecs {
  implicit def shortArrayTileCodec: AvroRecordCodec[ShortArrayTile] = new AvroRecordCodec[ShortArrayTile] {
    def schema = SchemaBuilder
      .record("ShortArrayTile").namespace("geotrellis.raster")
      .fields()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .name("cells").`type`().array().items().intType().noDefault()
      .endRecord()

    def encode(tile: ShortArrayTile, rec: GenericRecord) = {
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
      // _* expansion is important, otherwise we get List[Array[Short]] instead of List[Short]
      rec.put("cells", java.util.Arrays.asList(tile.array:_*))
    }

    def decode(rec: GenericRecord) = {
      val array  = rec.get("cells")
        .asInstanceOf[java.util.Collection[Int]]
        .asScala // notice that Avro does not have native support for Short primitive
        .map(_.toShort)
        .toArray
      new ShortArrayTile(array, rec[Int]("cols"), rec[Int]("rows"))
    }
  }

  implicit def uShortArrayTileCodec: AvroRecordCodec[UShortArrayTile] = new AvroRecordCodec[UShortArrayTile] {
    def schema = SchemaBuilder
      .record("UShortArrayTile").namespace("geotrellis.raster")
      .fields()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .name("cells").`type`().array().items().intType().noDefault()
      .endRecord()

    def encode(tile: UShortArrayTile, rec: GenericRecord) = {
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
      // _* expansion is important, otherwise we get List[Array[Short]] instead of List[Short]
      rec.put("cells", java.util.Arrays.asList(tile.array:_*))
    }

    def decode(rec: GenericRecord) = {
      val array  = rec.get("cells")
        .asInstanceOf[java.util.Collection[Int]]
        .asScala // notice that Avro does not have native support for Short primitive
        .map(_.toShort)
        .toArray
      new UShortArrayTile(array, rec[Int]("cols"), rec[Int]("rows"))
    }
  }

  implicit def intArrayTileCodec: AvroRecordCodec[IntArrayTile] = new AvroRecordCodec[IntArrayTile] {
    def schema = SchemaBuilder
      .record("IntArrayTile").namespace("geotrellis.raster")
      .fields()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .name("cells").`type`().array().items().intType().noDefault()
      .endRecord()

    def encode(tile: IntArrayTile, rec: GenericRecord) = {
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
      rec.put("cells", java.util.Arrays.asList(tile.array:_*))
    }

    def decode(rec: GenericRecord) = {
      val array  = rec.get("cells").asInstanceOf[java.util.Collection[Int]].asScala.toArray[Int]
      new IntArrayTile(array, rec[Int]("cols"), rec[Int]("rows"))
    }
  }

  implicit def floatArrayTileCodec: AvroRecordCodec[FloatArrayTile] = new AvroRecordCodec[FloatArrayTile] {
    def schema = SchemaBuilder
      .record("FloatArrayTile").namespace("geotrellis.raster")
      .fields()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .name("cells").`type`().array().items().floatType().noDefault()
      .endRecord()

    def encode(tile: FloatArrayTile, rec: GenericRecord) = {
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
      rec.put("cells", java.util.Arrays.asList(tile.array:_*))
    }

    def decode(rec: GenericRecord) = {
      val array  = rec.get("cells").asInstanceOf[java.util.Collection[Float]].asScala.toArray[Float]
      new FloatArrayTile(array, rec[Int]("cols"), rec[Int]("rows"))
    }
  }

  implicit def doubleArrayTileCodec: AvroRecordCodec[DoubleArrayTile] = new AvroRecordCodec[DoubleArrayTile] {
    def schema = SchemaBuilder
      .record("DoubleArrayTile").namespace("geotrellis.raster")
      .fields()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .name("cells").`type`().array().items().doubleType().noDefault()
      .endRecord()

    def encode(tile: DoubleArrayTile, rec: GenericRecord) = {
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
      rec.put("cells", java.util.Arrays.asList(tile.array:_*))
    }

    def decode(rec: GenericRecord) = {
      val array  = rec.get("cells").asInstanceOf[java.util.Collection[Double]].asScala.toArray[Double]
      new DoubleArrayTile(array, rec[Int]("cols"), rec[Int]("rows"))
    }
  }

  implicit def byteArrayTileCodec: AvroRecordCodec[ByteArrayTile] = new AvroRecordCodec[ByteArrayTile] {
    def schema = SchemaBuilder
      .record("ByteArrayTile").namespace("geotrellis.raster")
      .fields()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .name("cells").`type`().bytesType().noDefault()
      .endRecord()

    def encode(tile: ByteArrayTile, rec: GenericRecord) = {
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
      rec.put("cells", ByteBuffer.wrap(tile.array))
    }

    def decode(rec: GenericRecord) = {
      val array  = rec.get("cells").asInstanceOf[ByteBuffer].array()
      new ByteArrayTile(array, rec[Int]("cols"), rec[Int]("rows"))
    }
  }

  implicit def uByteArrayTileCodec: AvroRecordCodec[UByteArrayTile] = new AvroRecordCodec[UByteArrayTile] {
    def schema = SchemaBuilder
      .record("UByteArrayTile").namespace("geotrellis.raster")
      .fields()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .name("cells").`type`().bytesType().noDefault()
      .endRecord()

    def encode(tile: UByteArrayTile, rec: GenericRecord) = {
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
      rec.put("cells", ByteBuffer.wrap(tile.array))
    }

    def decode(rec: GenericRecord) = {
      val array  = rec.get("cells").asInstanceOf[ByteBuffer].array()
      new UByteArrayTile(array, rec[Int]("cols"), rec[Int]("rows"))
    }
  }

  implicit def bitArrayTileCodec: AvroRecordCodec[BitArrayTile] = new AvroRecordCodec[BitArrayTile] {
    def schema = SchemaBuilder
      .record("BitArrayTile").namespace("geotrellis.raster")
      .fields()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .name("cells").`type`().bytesType().noDefault()
      .endRecord()

    def encode(tile: BitArrayTile, rec: GenericRecord) = {
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
      rec.put("cells", ByteBuffer.wrap(tile.array))
    }

    def decode(rec: GenericRecord) = {
      val array  = rec.get("cells").asInstanceOf[ByteBuffer].array()
      new BitArrayTile(array, rec[Int]("cols"), rec[Int]("rows"))
    }
  }

  implicit def multibandTileCodec: AvroRecordCodec[MultibandTile] = new AvroRecordCodec[MultibandTile] {
    def schema = SchemaBuilder
      .record("ArrayMultibandTile").namespace("geotrellis.raster")
      .fields()
      .name("bands").`type`().array().items.`type`(tileUnionCodec.schema).noDefault()
      .endRecord()

    def encode(tile: MultibandTile, rec: GenericRecord) = {
      val bands = for (i <- 0 until tile.bandCount) yield tile.band(i)
      rec.put("bands", bands.map(tileUnionCodec.encode).asJavaCollection)
    }

    def decode(rec: GenericRecord) = {
      val bands = rec.get("bands")
        .asInstanceOf[java.util.Collection[GenericRecord]]
        .asScala // notice that Avro does not have native support for Short primitive
        .map(tileUnionCodec.decode)
        .toArray

      new ArrayMultibandTile(bands)
    }
  }
}

object TileCodecs extends TileCodecs