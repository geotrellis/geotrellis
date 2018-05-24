/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.io.cog

import io.circe._

import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.util._

import java.net.URI
import java.util.ServiceLoader

import scala.reflect._

trait COGValueReader[ID] {
  val attributeStore: AttributeStore

  implicit def getByteReader(uri: URI): ByteReader
  implicit def getLayerId(id: ID): LayerId

  def reader[
    K: Decoder : SpatialComponent : ClassTag,
    V <: CellGrid : GeoTiffReader
  ](layerId: LayerId): Reader[K, V]

  /** Produce a key value reader for a specific layer, prefetching layer metadata once at construction time */
  def baseReader[
    K: Decoder: SpatialComponent: ClassTag,
    V <: CellGrid: GeoTiffReader
  ](
    layerId: ID,
    keyPath: (K, Int, KeyIndex[K], ZoomRange) => String, // Key, maxWidth, toIndex, zoomRange
    fullPath: String => URI,
    exceptionHandler: K => PartialFunction[Throwable, Nothing] = { key: K => { case e: Throwable => throw e }: PartialFunction[Throwable, Nothing] }
   ): Reader[K, V] = new Reader[K, V] {
    val COGLayerStorageMetadata(cogLayerMetadata, keyIndexes) =
      attributeStore.readMetadata[COGLayerStorageMetadata[K]](LayerId(layerId.name, 0))

    def read(key: K): V = {
      val (zoomRange, spatialKey, overviewIndex, gridBounds) =
        cogLayerMetadata.getReadDefinition(key.getComponent[SpatialKey], layerId.zoom)

      val baseKeyIndex = keyIndexes(zoomRange)

      val maxWidth = Index.digits(baseKeyIndex.toIndex(baseKeyIndex.keyBounds.maxKey))
      val uri = fullPath(keyPath(key.setComponent(spatialKey), maxWidth, baseKeyIndex, zoomRange))

      try {
        GeoTiffReader[V].read(uri, streaming = true)
          .getOverview(overviewIndex)
          .crop(gridBounds)
          .tile
      } catch {
        case th: Throwable => exceptionHandler(key)(th)
      }
    }
  }

  def overzoomingReader[
    K: Decoder: SpatialComponent: ClassTag,
    V <: CellGrid: GeoTiffReader: ? => TileResampleMethods[V]
  ](layerId: ID, resampleMethod: ResampleMethod = ResampleMethod.DEFAULT): Reader[K, V]
}

object COGValueReader {

  /**
   * Produce COGValueReader instance based on URI description.
   * Find instances of [[COGValueReaderProvider]] through Java SPI.
   */
  def apply(attributeStore: AttributeStore, valueReaderUri: URI): COGValueReader[LayerId] = {
    import scala.collection.JavaConversions._
    ServiceLoader.load(classOf[COGValueReaderProvider]).iterator()
      .find(_.canProcess(valueReaderUri))
      .getOrElse(throw new RuntimeException(s"Unable to find COGValueReaderProvider for $valueReaderUri"))
      .valueReader(valueReaderUri, attributeStore)
  }

  /**
   * Produce COGValueReader instance based on URI description.
   * Find instances of [[COGValueReaderProvider]] through Java SPI.
   */
  def apply(attributeStoreUri: URI, valueReaderUri: URI): COGValueReader[LayerId] =
    apply(AttributeStore(attributeStoreUri), valueReaderUri)

  /**
   * Produce COGValueReader instance based on URI description.
   * Find instances of [[COGValueReaderProvider]] through Java SPI.
   * Required [[AttributeStoreProvider]] instance will be found from the same URI.
   */
  def apply(uri: URI): COGValueReader[LayerId] =
    apply(attributeStoreUri = uri, valueReaderUri = uri)

  def apply(attributeStore: AttributeStore, valueReaderUri: String): COGValueReader[LayerId] =
    apply(attributeStore, new URI(valueReaderUri))

  def apply(attributeStoreUri: String, valueReaderUri: String): COGValueReader[LayerId] =
    apply(AttributeStore(new URI(attributeStoreUri)), new URI(valueReaderUri))

  def apply(uri: String): COGValueReader[LayerId] = {
    val _uri = new URI(uri)
    apply(attributeStoreUri = _uri, valueReaderUri = _uri)
  }
}
