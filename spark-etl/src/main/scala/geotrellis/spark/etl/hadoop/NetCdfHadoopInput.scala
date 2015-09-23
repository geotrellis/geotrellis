package geotrellis.spark.etl.hadoop

import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.spark.ingest._
import geotrellis.spark.reproject._
import geotrellis.spark.io.hadoop.formats.NetCdfBand
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling.LayoutScheme
import geotrellis.spark.{SpaceTimeKey, RasterRDD, RasterMetaData, SpatialKey}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import scala.reflect._

class NetCdfHadoopInput extends HadoopInput {
  val format = "netcdf"
  val key = classTag[SpaceTimeKey]

  def apply[K](lvl: StorageLevel, crs: CRS, layoutScheme: LayoutScheme, props: Map[String, String])(implicit sc: SparkContext) = {
    val source = sc.netCdfRDD(new Path(props("path")))
    val reprojected = source.reproject(crs).persist(lvl)
    val (layoutLevel, rasterMetaData) =
      RasterMetaData.fromRdd(reprojected, crs, layoutScheme) { _.extent }
    val tiler = implicitly[Tiler[NetCdfBand, SpaceTimeKey, Tile]]
    layoutLevel -> tiler(reprojected, rasterMetaData, NearestNeighbor).asInstanceOf[RasterRDD[K]]
  }
}