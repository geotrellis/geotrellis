package geotrellis.spark.io.hadoop

import geotrellis.spark._
import geotrellis.spark.ingest.ProjectedExtent
import geotrellis.spark.io.hadoop.formats._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4.CRS

import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.hadoop.fs.Path

import scala.reflect._

trait HadoopSparkContextMethods {
  val sc: SparkContext

  def hadoopGeoTiffRDD(path: String): RDD[(ProjectedExtent, Tile)] =
    hadoopGeoTiffRDD(new Path(path))

  def hadoopGeoTiffRDD(path: Path): RDD[(ProjectedExtent, Tile)] = {
    val updatedConf =
      sc.hadoopConfiguration.withInputDirectory(path)

    sc.newAPIHadoopRDD(
      updatedConf,
      classOf[GeotiffInputFormat],
      classOf[ProjectedExtent],
      classOf[Tile]
    )
  }

  def gdalRDD(path: Path): RDD[(GdalRasterInfo, Tile)] = {
    val updatedConf = sc.hadoopConfiguration.withInputDirectory(path)

    sc.newAPIHadoopRDD(
      updatedConf,
      classOf[GdalInputFormat],
      classOf[GdalRasterInfo],
      classOf[Tile]
    )
  }

  def netCdfRDD(path: Path): RDD[(NetCdfBand, Tile)] =
    gdalRDD(path)
      .map { case (info, tile) =>
        val band = NetCdfBand(
          extent = info.file.rasterExtent.extent,
          crs = info.file.crs,
          varName = info.bandMeta("NETCDF_VARNAME"),
          time = info.bandMeta("NETCDF_DIM_Time").toDouble
        )
        band -> tile
    }
}
