package geotrellis.spark.io.s3

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.util._
import geotrellis.spark.io.s3.util.S3BytesStreamer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.amazonaws.services.s3.model._

object TemporialGeoTiffRDD {
  import GeoTiffReaderExtensions.Reader

  def apply[K, T: Reader](bucket: String, prefix: String)
    (implicit s3Client: S3Client, sc: SparkContext): RDD[(TemporalProjectedExtent, T)] =
      apply(bucket, prefix, None)
  
  def apply[K, T: Reader](bucket: String, prefix: String, maxTileDimensions: (Int, Int))
    (implicit s3Client: S3Client, sc: SparkContext): RDD[(TemporalProjectedExtent, T)] =
    apply(bucket, prefix, Some(maxTileDimensions))

  def apply[T: Reader](bucket: String, prefix: String, maxTileDimensions: Option[(Int, Int)])
    (implicit s3Client: S3Client, sc: SparkContext): RDD[(TemporalProjectedExtent, T)] =
    GeoTiffRDD.apply(bucket, prefix, maxTileDimensions)((_, _, geoTiff) => geoTiff.projectedRaster.projectedExtent)
}
