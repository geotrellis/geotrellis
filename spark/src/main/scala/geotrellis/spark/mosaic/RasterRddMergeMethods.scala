package geotrellis.spark.mosaic

import scala.reflect.ClassTag
import geotrellis.spark._

class RasterRddMergeMethods[K: SpatialComponent: ClassTag](rdd: RasterRDD[K]) extends MergeMethods[RasterRDD[K]] {
 def merge(other: RasterRDD[K]) = {
   val (outRdd, _) = (rdd.tileRdd, rdd.metaData.layout).merge(other.tileRdd, rdd.metaData.layout)
   new RasterRDD(outRdd, rdd.metaData)
 }
}
