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

import geotrellis.spark.tiling._
import geotrellis.spark.ingest._

import org.apache.spark.rdd._

import spire.syntax.cfor._

import monocle.{Lens, PLens}
import monocle.syntax._

import scala.reflect.ClassTag

package object spark extends buffer.Implicits 
    with reproject.Implicits {

  type RasterRDD[K] = RDD[(K, Tile)] with Metadata[RasterMetaData]
  object RasterRDD {
    def apply[K](rdd: RDD[(K, Tile)], metadata: RasterMetaData): RasterRDD[K] =
      new ContextRDD(rdd, metadata)
  }

  type MultiBandRasterRDD[K] = RDD[(K, MultiBandTile)] with Metadata[RasterMetaData]
  object MultiBandRasterRDD {
    def apply[K](rdd: RDD[(K, MultiBandTile)], metadata: RasterMetaData): MultiBandRasterRDD[K] =
      new ContextRDD(rdd, metadata)
  }

  type ComponentLens[K, C] = PLens[K, K, C, C]

  type SpatialComponent[K] = KeyComponent[K, SpatialKey]
  type TemporalComponent[K] = KeyComponent[K, TemporalKey]

  implicit class SpatialComponentWrapper[K: SpatialComponent](key: K) {
    val _spatialComponent = implicitly[SpatialComponent[K]]

    def spatialComponent: SpatialKey = key &|-> _spatialComponent.lens get

    def updateSpatialComponent(spatialKey: SpatialKey): K =
      key &|-> _spatialComponent.lens set(spatialKey)
  }

  implicit class TemporalCompenentWrapper[K: TemporalComponent](key: K) {
    val _temporalComponent = implicitly[TemporalComponent[K]]

    def temporalComponent: TemporalKey = key &|-> _temporalComponent.lens get

    def updateTemporalComponent(temporalKey: TemporalKey): K =
      key &|-> _temporalComponent.lens set(temporalKey)
  }

  type TileBounds = GridBounds

  implicit class toPipe[A](x : A) {
    def |> [T](f : A => T) = f(x)
  }

  implicit class toPipe2[A, B](tup : (A, B)) {
    def |> [T](f : (A, B) => T) = f(tup._1, tup._2)
  }

  implicit class toPipe3[A, B, C](tup : (A, B, C)) {
    def |> [T](f : (A, B, C) => T) = f(tup._1, tup._2, tup._3)
  }

  implicit class toPipe4[A, B, C, D](tup : (A, B, C, D)) {
    def |> [T](f : (A, B, C, D) => T) = f(tup._1, tup._2, tup._3, tup._4)
  }

  implicit class WithContextWrapper[K, V, M](val rdd: RDD[(K, V)] with Metadata[M]) {
    def withContext[K2, V2](f: RDD[(K, V)] => RDD[(K2, V2)]) =
      new ContextRDD(f(rdd), rdd.metadata)
  }

  implicit def tupleToRDDWithMetadata[K, V, M](tup: (RDD[(K, V)], M)): RDD[(K, V)] with Metadata[M] =
    ContextRDD(tup._1, tup._2)

  implicit class withContextRDDMethods[K: ClassTag, V: ClassTag, M](rdd: RDD[(K, V)] with Metadata[M])
    extends ContextRDDMethods[K, V, M](rdd)

  implicit class withRasterRDDMethods[K](val rasterRDD: RasterRDD[K])(implicit val keyClassTag: ClassTag[K])
    extends BaseRasterRDDMethods[K]

  implicit class withSpatialRasterRDDMethods(val rdd: RasterRDD[SpatialKey]) extends SpatialRasterRDDMethods

  implicit class withMultiBandRasterRDDMethods[K](val rdd: MultiBandRasterRDD[K])(implicit val keyClassTag: ClassTag[K])
    extends BaseMultiBandRasterRDDMethods[K]

  implicit class withIngestKeyRDDMethods[K: IngestKey, V <: CellGrid](val rdd: RDD[(K, V)]) {
    def toRasters: RDD[(K, Raster[V])] =
      rdd.mapPartitions({ partition =>
        partition.map { case (key, value) =>
          (key, Raster(value, key.projectedExtent.extent))
        }
      }, preservesPartitioning = true)
  }

  /** Keeps with the convention while still using simple tups, nice */
  implicit class TileTuple[K](tup: (K, Tile)) {
    def id: K = tup._1
    def tile: Tile = tup._2
  }

  implicit class RDDTraversableExtensions[K: ClassTag, V, M](rs: Traversable[RDD[(K, Tile)] with Metadata[M]]) {
    def combinePairs(f: (Traversable[(K, Tile)] => (K, Tile))): RDD[(K, Tile)] with Metadata[M] =
      rs.head.combinePairs(rs.tail)(f)
  }
}
