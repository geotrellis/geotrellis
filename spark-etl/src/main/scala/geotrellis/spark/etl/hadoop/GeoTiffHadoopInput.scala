package geotrellis.spark.etl.hadoop

import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.etl.config.EtlConf
import geotrellis.vector.ProjectedExtent
import geotrellis.spark.ingest._
import geotrellis.spark.merge._
import geotrellis.spark.io.hadoop._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class GeoTiffHadoopInput extends HadoopInput[ProjectedExtent, Tile]() {
  val format = "geotiff"
  def apply(conf: EtlConf)(implicit sc: SparkContext): RDD[(ProjectedExtent, Tile)] =
    HadoopGeoTiffRDD.spatial(getPath(conf.input.backend).path, HadoopGeoTiffRDD.Options(crs = conf.input.getCrs))
}
