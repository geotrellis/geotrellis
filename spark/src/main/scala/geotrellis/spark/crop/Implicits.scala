package geotrellis.spark.crop

import geotrellis.raster._
import geotrellis.spark._


object Implicits extends Implicits

trait Implicits {
  implicit class withRasterRDDCropMethods[K: SpatialComponent](val self: RasterRDD[K])
      extends RasterRDDCropMethods[K]
}
