package geotrellis.spark.etl.hadoop

import geotrellis.proj4.CRS
import geotrellis.raster.MultibandTile
import geotrellis.spark.etl.config.EtlConf
import geotrellis.vector.ProjectedExtent
import geotrellis.spark.io.hadoop._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class MultibandGeoTiffHadoopInput extends HadoopInput[ProjectedExtent, MultibandTile] {
  val format = "multiband-geotiff"
  def apply(conf: EtlConf)(implicit sc: SparkContext): RDD[(ProjectedExtent, MultibandTile)] =
    HadoopGeoTiffRDD.spatialMultiband(getPath(conf.input.backend).path, HadoopGeoTiffRDD.Options(crs = conf.input.getCrs))
}
