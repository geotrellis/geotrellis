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

package geotrellis.spark.io.s3.cog

import geotrellis.raster.{CellGrid, GridBounds, RasterExtent}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.cog._
import geotrellis.spark.io.index._
import geotrellis.spark.tiling.LayoutLevel
import geotrellis.util._
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import spray.json.JsonFormat

import scala.reflect.ClassTag

/**
 * Handles reading raster RDDs and their metadata from S3.
 *
 * @param attributeStore  AttributeStore that contains metadata for corresponding LayerId
 */
class S3COGLayerReader(val attributeStore: AttributeStore)(implicit sc: SparkContext)
  extends FilteringCOGLayerReader[LayerId] with LazyLogging {

  val defaultNumPartitions: Int = sc.defaultParallelism

  def read[
    K: SpatialComponent: Boundable: JsonFormat: ClassTag,
    V <: CellGrid: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]
  ](id: LayerId, tileQuery: LayerQuery[K, M], numPartitions: Int, filterIndexOnly: Boolean) = {
    val rddReader = S3COGRDDReader.fromRegistry[V]
    if(!attributeStore.layerExists(id)) throw new LayerNotFoundError(id)

    val LayerAttributes(header, metadata, _, _) = try {
      attributeStore.readLayerAttributes[S3COGLayerHeader, M, K](id)
    } catch {
      case e: AttributeNotFoundError => throw new LayerReadError(id).initCause(e)
    }

    val LayerAttributes(_, baseMetadata, baseKeyIndex, _) = try {
      attributeStore.readLayerAttributes[S3COGLayerHeader, M, K](id.copy(zoom = header.zoomRanges._1))
    } catch {
      case e: AttributeNotFoundError => throw new LayerReadError(id).initCause(e)
    }

    val bucket = header.bucket
    val prefix = header.key

    val queryKeyBounds: Seq[KeyBounds[K]] = tileQuery(metadata)
    val maxWidth = Index.digits(baseKeyIndex.toIndex(baseKeyIndex.keyBounds.maxKey))
    val keyPath = (index: BigInt) => makePath(prefix, Index.encode(index, maxWidth))
    val decompose = (bounds: KeyBounds[K]) => baseKeyIndex.indexRanges(bounds)
    val layoutScheme = header.layoutScheme

    val LayoutLevel(_, baseLayout) = layoutScheme.levelForZoom(header.zoomRanges._1)
    val LayoutLevel(_, layout) = layoutScheme.levelForZoom(id.zoom)

    val baseKeyBounds = baseMetadata.getComponent[Bounds[K]]

    def transformKeyBounds(keyBounds: KeyBounds[K]): KeyBounds[K] = {
      val KeyBounds(minKey, maxKey) = keyBounds
      val extent = layout.extent
      val sourceRe = RasterExtent(extent, layout.layoutCols, layout.layoutRows)
      val targetRe = RasterExtent(extent, baseLayout.layoutCols, baseLayout.layoutRows)

      val minSpatialKey = minKey.getComponent[SpatialKey]
      val (minCol, minRow) = {
        val (x, y) = sourceRe.gridToMap(minSpatialKey.col, minSpatialKey.row)
        targetRe.mapToGrid(x, y)
      }

      val maxSpatialKey = maxKey.getComponent[SpatialKey]
      val (maxCol, maxRow) = {
        val (x, y) = sourceRe.gridToMap(maxSpatialKey.col, maxSpatialKey.row)
        targetRe.mapToGrid(x, y)
      }

      KeyBounds(
        minKey.setComponent(SpatialKey(minCol, minRow)),
        maxKey.setComponent(SpatialKey(maxCol, maxRow))
      )
    }

    val baseQueryKeyBounds: Seq[KeyBounds[K]] =
      queryKeyBounds
        .flatMap { qkb =>
          transformKeyBounds(qkb).intersect(baseKeyBounds) match {
            case EmptyBounds => None
            case kb: KeyBounds[K] => Some(kb)
          }
        }
        .distinct

    // overview index basing on the partial pyramid zoom ranges
    val overviewIndex = header.zoomRanges._2 - id.zoom - 1

    val rdd = rddReader.read[K](
      bucket             = bucket,
      keyPath            = keyPath,
      baseQueryKeyBounds = baseQueryKeyBounds,
      realQueryKeyBounds = queryKeyBounds,
      decomposeBounds    = decompose,
      sourceLayout       = layout,
      overviewIndex      = overviewIndex,
      numPartitions      = Some(numPartitions)
    )

    new ContextRDD(rdd, metadata)
  }
}

object S3COGLayerReader {
  def apply(attributeStore: AttributeStore)(implicit sc: SparkContext): S3COGLayerReader =
    new S3COGLayerReader(attributeStore)

  def apply(bucket: String, prefix: String)(implicit sc: SparkContext): S3COGLayerReader =
    apply(new S3AttributeStore(bucket, prefix))
}
