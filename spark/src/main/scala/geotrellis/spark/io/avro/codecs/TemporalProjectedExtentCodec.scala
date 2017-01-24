package geotrellis.spark.io.avro.codecs

import geotrellis.proj4.CRS
import geotrellis.spark._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.vector._

import org.apache.avro._
import org.apache.avro.generic._


trait TemporalProjectedExtentCodec {
  implicit def temporalProjectedExtentCodec = new AvroRecordCodec[TemporalProjectedExtent] {
    def schema: Schema = SchemaBuilder
      .record("TemporalProjectedExtent").namespace("geotrellis.spark")
      .fields()
      .name("extent").`type`(extentCodec.schema).noDefault()
      .name("proj4").`type`().stringType().noDefault()
      .name("instant").`type`().longType().noDefault()
      .endRecord()

    def encode(temporalProjectedExtent: TemporalProjectedExtent, rec: GenericRecord): Unit = {
      rec.put("extent", extentCodec.encode(temporalProjectedExtent.extent))
      rec.put("proj4", temporalProjectedExtent.crs.toProj4String)
      rec.put("instant", temporalProjectedExtent.instant)
    }

    def decode(rec: GenericRecord): TemporalProjectedExtent = {
      val instant = rec[Long]("instant")

      val proj4 = rec.get("proj4").toString()
      val crs = CRS.fromString(proj4)

      val extent = extentCodec.decode(rec[GenericRecord]("extent"))

      TemporalProjectedExtent(extent, crs, instant)
    }
  }
}
