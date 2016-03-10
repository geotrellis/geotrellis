package geotrellis.spark.etl.hadoop

import geotrellis.raster.MultibandTile
import geotrellis.spark.ingest._
import geotrellis.spark.io.hadoop._
import geotrellis.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class TemporalMultibandGeoTiffHadoopInput extends HadoopInput[TemporalProjectedExtent, MultibandTile] {
  val format = "temporal-geotiff"
  def apply(props: Parameters)(implicit sc: SparkContext): RDD[(TemporalProjectedExtent, MultibandTile)] = sc.hadoopTemporalMultibandGeoTiffRDD(props("path"))
}

