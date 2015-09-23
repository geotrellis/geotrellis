package geotrellis.spark.etl.s3

import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.spark.ingest._
import geotrellis.spark.reproject._
import geotrellis.spark.io.s3.GeoTiffS3InputFormat
import geotrellis.spark.tiling.LayoutScheme
import geotrellis.spark.{RasterRDD, RasterMetaData, SpatialKey}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import scala.reflect._

class GeoTiffS3Input extends S3Input {
  val format = "geotiff"
  val key = classTag[SpatialKey]

  def apply[K](lvl: StorageLevel, crs: CRS, layoutScheme: LayoutScheme, props: Map[String, String])(implicit sc: SparkContext) = {
    val source = sc.newAPIHadoopRDD(configuration(props), classOf[GeoTiffS3InputFormat], classOf[ProjectedExtent], classOf[Tile])
    val reprojected = source.reproject(crs).persist(lvl)
    val (layoutLevel, rasterMetaData) =
      RasterMetaData.fromRdd(reprojected, crs, layoutScheme) { _.extent }
    val tiler = implicitly[Tiler[ProjectedExtent, SpatialKey, Tile]]
    layoutLevel -> tiler(reprojected, rasterMetaData, NearestNeighbor).asInstanceOf[RasterRDD[K]]
  }
}

