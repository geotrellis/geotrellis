package geotrellis.spark.io.s3

import geotrellis.util.StreamByteReader
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.spark._
import geotrellis.spark.io.s3.util.S3BytesStreamer
import geotrellis.vector.Extent
import org.apache.hadoop.mapreduce._

import java.time.ZonedDateTime

/** Read single band GeoTiff from S3
  *
  * This can be configured with the hadoop configuration by providing:
  * TemporalMultibandGeoTiffS3InputFormat.GEOTIFF_TIME_TAG; default of "TIFFTAG_DATETIME"
  * TemporalMultibandGeoTiffS3InputFormat.GEOTIFF_TIME_FORMAT; default is ""yyyy:MM:DD HH:MM:SS""
  */
class TemporalMultibandGeoTiffS3InputFormat extends S3InputFormat[TemporalProjectedExtent, MultibandTile] {
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) =
    new S3RecordReader[TemporalProjectedExtent, MultibandTile] {
      def read(key: String, bytes: Array[Byte]) = {
        val geoTiff = MultibandGeoTiff(bytes)
        toProjectedRaster(geoTiff)
      }
      
      def read(key: String, bytes: S3BytesStreamer) =
        read(key, None, bytes)
      
      def read(key: String, e: Extent, bytes: S3BytesStreamer) =
        read(key, Some(e), bytes)

      def read(key: String, e: Option[Extent], bytes: S3BytesStreamer) = {
        val reader = StreamByteReader(bytes)
        val geoTiff = MultibandGeoTiff(reader, e)
        toProjectedRaster(geoTiff)
      }

      private def toProjectedRaster(geoTiff: MultibandGeoTiff) = {
        val timeTag = TemporalGeoTiffS3InputFormat.getTimeTag(context)
        val dateFormatter = TemporalGeoTiffS3InputFormat.getTimeFormatter(context)

        val dateTimeString = geoTiff.tags.headTags.getOrElse(timeTag, sys.error(s"There is no tag $timeTag in the GeoTiff header"))
        val dateTime = ZonedDateTime.parse(dateTimeString, dateFormatter)

        val ProjectedRaster(Raster(tile, extent), crs) = geoTiff.projectedRaster
        (TemporalProjectedExtent(extent, crs, dateTime), tile)
      }
    }
}
