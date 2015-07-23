package geotrellis.spark.op.local

import geotrellis.spark._
import geotrellis.raster.Tile
import reflect.ClassTag

package object temporal {

  implicit class LocalTemporalRasterRDDExtensions[K](val rasterRDD: RasterRDD[K, Tile])(
    implicit val keyClassTag: ClassTag[K],
    implicit val _sc: SpatialComponent[K],
    implicit val _tc: TemporalComponent[K]) extends LocalTemporalRasterRDDMethods[K] { }

  implicit class TemporalWindow[K](val rasterRDD: RasterRDD[K, Tile])(
    implicit val keyClassTag: ClassTag[K],
    _sc: SpatialComponent[K],
    _tc: TemporalComponent[K]) {

    import TemporalWindowHelper._

    def average: TemporalWindowState[K] = TemporalWindowState(rasterRDD, Average)

    def minimum: TemporalWindowState[K] = TemporalWindowState(rasterRDD, Minimum)

    def maximum: TemporalWindowState[K] = TemporalWindowState(rasterRDD, Maximum)

    def variance: TemporalWindowState[K] = TemporalWindowState(rasterRDD, Variance)

  }

}
