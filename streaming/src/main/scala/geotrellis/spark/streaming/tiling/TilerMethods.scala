package geotrellis.spark.streaming.tiling

import geotrellis.raster._
import geotrellis.raster.merge._
import geotrellis.raster.prototype._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.util.MethodExtensions

import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

class TilerMethods[K, V <: CellGrid: ClassTag: (? => TileMergeMethods[V]): (? => TilePrototypeMethods[V])](val self: DStream[(K, V)]) extends MethodExtensions[DStream[(K, V)]] {
  import Tiler.Options

  def cutTiles[K2: SpatialComponent: ClassTag](cellType: CellType, layoutDefinition: LayoutDefinition, resampleMethod: ResampleMethod)
                                              (implicit ev: K => TilerKeyMethods[K, K2]): DStream[(K2, V)] =
    self.transform(CutTiles[K, K2, V](_, cellType, layoutDefinition, resampleMethod))

  def cutTiles[K2: SpatialComponent: ClassTag](cellType: CellType, layoutDefinition: LayoutDefinition)
                                              (implicit ev: K => TilerKeyMethods[K, K2]): DStream[(K2, V)] =
    cutTiles(cellType, layoutDefinition, NearestNeighbor)

  def cutTiles[K2: SpatialComponent: ClassTag](tileLayerMetadata: TileLayerMetadata[K2], resampleMethod: ResampleMethod)
                                              (implicit ev: K => TilerKeyMethods[K, K2]): DStream[(K2, V)] =
    cutTiles(tileLayerMetadata.cellType, tileLayerMetadata.layout, resampleMethod)

  def cutTiles[K2: SpatialComponent: ClassTag](tileLayerMetadata: TileLayerMetadata[K2])
                                              (implicit ev: K => TilerKeyMethods[K, K2]): DStream[(K2, V)] =
    cutTiles(tileLayerMetadata, NearestNeighbor)

  def tileToLayout[K2: SpatialComponent: ClassTag](cellType: CellType, layoutDefinition: LayoutDefinition, options: Options)
                                                  (implicit ev: K => TilerKeyMethods[K, K2]): DStream[(K2, V)] =
    self.transform(CutTiles[K, K2, V](_, cellType, layoutDefinition, options.resampleMethod).merge(options.partitioner))

  def tileToLayout[K2: SpatialComponent: ClassTag](cellType: CellType, layoutDefinition: LayoutDefinition)
                                                  (implicit ev: K => TilerKeyMethods[K, K2]): DStream[(K2, V)] =
    tileToLayout(cellType, layoutDefinition, Options.DEFAULT)

  def tileToLayout[K2: SpatialComponent: ClassTag](tileLayerMetadata: TileLayerMetadata[K2], options: Options)
                                                  (implicit ev: K => TilerKeyMethods[K, K2]): DStream[(K2, V)] =
    tileToLayout(tileLayerMetadata.cellType, tileLayerMetadata.layout, options)

  def tileToLayout[K2: SpatialComponent: ClassTag](tileLayerMetadata: TileLayerMetadata[K2])
                                                  (implicit ev: K => TilerKeyMethods[K, K2]): DStream[(K2, V)] =
    tileToLayout(tileLayerMetadata, Options.DEFAULT)

}
