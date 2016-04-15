/*
 * Copyright (c) 2014 DigitalGlobe.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis

import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.util._

import geotrellis.spark.tiling._
import geotrellis.spark.ingest._
import geotrellis.spark.crop._
import geotrellis.spark.filter._

import org.apache.spark.Partitioner
import org.apache.spark.rdd._

import spire.syntax.cfor._

import monocle._
import monocle.syntax._

import scala.reflect.ClassTag
import scalaz.Functor

package object spark
    extends buffer.Implicits
    with crop.Implicits
    with filter.Implicits
    with join.Implicits
    with mapalgebra.Implicits
    with mapalgebra.local.Implicits
    with mapalgebra.local.temporal.Implicits
    with mapalgebra.focal.Implicits
    with mapalgebra.focal.hillshade.Implicits
    with mapalgebra.zonal.Implicits
    with mask.Implicits
    with merge.Implicits
    with partition.Implicits
    with resample.Implicits
    with reproject.Implicits
    with split.Implicits
    with stitch.Implicits
    with summary.polygonal.Implicits
    with summary.Implicits
    with tiling.Implicits {
  type TileLayerRDD[K] = RDD[(K, Tile)] with Metadata[TileLayerMetadata[K]]

  object TileLayerRDD {
    def apply[K](rdd: RDD[(K, Tile)], metadata: TileLayerMetadata[K]): TileLayerRDD[K] =
      new ContextRDD(rdd, metadata)
  }

  type MultibandTileLayerRDD[K] = RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]
  object MultibandTileLayerRDD {
    def apply[K](rdd: RDD[(K, MultibandTile)], metadata: TileLayerMetadata[K]): MultibandTileLayerRDD[K] =
      new ContextRDD(rdd, metadata)
  }

  trait Component[T, C] extends GetComponent[T, C] with SetComponent[T, C]

  trait GetComponent[T, C] extends Serializable {
    def get: T => C
  }

  trait SetComponent[T, C] extends Serializable {
    def set: (T, C) => T
  }

  object Component {
    def apply[T, C](_get: T => C, _set: (T, C) => T): Component[T, C] =
      new Component[T, C] {
        val get = _get
        val set = _set
      }
  }

  object GetComponent {
    def apply[T, C](_get: T => C): GetComponent[T, C] =
      new GetComponent[T, C] {
        val get = _get
      }
  }


  object SetComponent {
    def apply[T, C](_set: (T, C) => T): SetComponent[T, C] =
      new SetComponent[T, C] {
        val set = _set
      }
  }

  implicit def identityComponent[T]: Component[T, T] =
    Component(v => v, (_, v) => v)

  /** Describes a getter and setter for an object that has
    * an implicitly defined lens into a component of that object
    * with a specific type.
    */
  implicit class withGetComponentMethods[T](val self: T) extends MethodExtensions[T] {
    def getComponent[C]()(implicit component: GetComponent[T, C]): C =
      component.get(self)
  }

  implicit class withSetComponentMethods[T](val self: T) extends MethodExtensions[T] {
    def setComponent[C](value: C)(implicit component: SetComponent[T, C]): T =
      component.set(self, value)
  }

  type SpatialComponent[K] = Component[K, SpatialKey]
  type TemporalComponent[K] = Component[K, TemporalKey]

  type TileBounds = GridBounds

  /** Auto wrap a partitioner when something is requestion an Option[Partitioner];
    * useful for Options that take an Option[Partitioner]
    */
  implicit def partitionerToOption(partitioner: Partitioner): Option[Partitioner] =
    Some(partitioner)

  implicit class WithContextWrapper[K, V, M](val rdd: RDD[(K, V)] with Metadata[M]) {
    def withContext[K2, V2](f: RDD[(K, V)] => RDD[(K2, V2)]) =
      new ContextRDD(f(rdd), rdd.metadata)

    def mapContext[M2](f: M => M2) =
      new ContextRDD(rdd, f(rdd.metadata))
  }

  implicit def tupleToRDDWithMetadata[K, V, M](tup: (RDD[(K, V)], M)): RDD[(K, V)] with Metadata[M] =
    ContextRDD(tup._1, tup._2)

  implicit class withContextRDDMethods[K: ClassTag, V: ClassTag, M](rdd: RDD[(K, V)] with Metadata[M])
      extends ContextRDDMethods[K, V, M](rdd)

  implicit class withTileLayerRDDMethods[K: SpatialComponent: ClassTag](val self: TileLayerRDD[K])
      extends TileLayerRDDMethods[K]

  implicit class withTileLayerRDDMaskMethods[K: SpatialComponent: ClassTag](val self: TileLayerRDD[K])
      extends mask.TileLayerRDDMaskMethods[K]

  implicit class withMultibandTileLayerRDDMethods[K: SpatialComponent: ClassTag](val self: MultibandTileLayerRDD[K])
      extends MultibandTileLayerRDDMethods[K]

  implicit class withCellGridLayoutRDDMethods[K: SpatialComponent: ClassTag, V <: CellGrid, M: GetComponent[?, LayoutDefinition]](val self: RDD[(K, V)] with Metadata[M])
      extends CellGridLayoutRDDMethods[K, V, M]

  implicit class withProjectedExtentRDDMethods[K: Component[?, ProjectedExtent], V <: CellGrid](val rdd: RDD[(K, V)]) {
    def toRasters: RDD[(K, Raster[V])] =
      rdd.mapPartitions({ partition =>
        partition.map { case (key, value) =>
          (key, Raster(value, key.getComponent[ProjectedExtent].extent))
        }
      }, preservesPartitioning = true)
  }

  implicit class withProjectedExtentTemporalTilerKeyMethods[K: Component[?, ProjectedExtent]: Component[?, TemporalKey]](val self: K) extends TilerKeyMethods[K, SpaceTimeKey] {
    def extent = self.getComponent[ProjectedExtent].extent
    def translate(spatialKey: SpatialKey): SpaceTimeKey = SpaceTimeKey(spatialKey, self.getComponent[TemporalKey])
  }

  implicit class withProjectedExtentTilerKeyMethods[K: Component[?, ProjectedExtent]](val self: K) extends TilerKeyMethods[K, SpatialKey] {
    def extent = self.getComponent[ProjectedExtent].extent
    def translate(spatialKey: SpatialKey) = spatialKey
  }

  implicit class withCollectMetadataMethods[K1, V <: CellGrid](rdd: RDD[(K1, V)]) extends Serializable {
    def collectMetadata[K2: Boundable: SpatialComponent](crs: CRS, layoutScheme: LayoutScheme)
        (implicit ev: K1 => TilerKeyMethods[K1, K2]): (Int, TileLayerMetadata[K2]) = {
      TileLayerMetadata.fromRdd(rdd, crs, layoutScheme)
    }

    def collectMetadata[K2: Boundable: SpatialComponent](crs: CRS, layout: LayoutDefinition)
        (implicit ev: K1 => TilerKeyMethods[K1, K2]): TileLayerMetadata[K2] = {
      TileLayerMetadata.fromRdd(rdd, crs, layout)
    }
  }
}
