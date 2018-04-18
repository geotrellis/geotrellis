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

package geotrellis.spark.io.accumulo

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.tiling._
import geotrellis.util._
import geotrellis.vector._

import org.apache.accumulo.core.data.{Range => AccumuloRange}
import org.apache.hadoop.io.Text
import spray.json._

import scala.reflect._

class AccumuloCollectionLayerReader(val attributeStore: AttributeStore)(implicit instance: AccumuloInstance) extends CollectionLayerReader[LayerId] {

  def read[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag: SpatialComponent,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: Component[?, Bounds[K]]: Component[?, LayoutDefinition]: Component[?, Extent]
  ](id: LayerId, rasterQuery: LayerQuery[K, M], filterIndexOnly: Boolean) = {
    if (!attributeStore.layerExists(id)) throw new LayerNotFoundError(id)

    val LayerAttributes(header, metadata, keyIndex, writerSchema) = try {
      attributeStore.readLayerAttributes[AccumuloLayerHeader, M, K](id)
    } catch {
      case e: AttributeNotFoundError => throw new LayerReadError(id).initCause(e)
    }

    val queryKeyBounds = rasterQuery(metadata)

    val layerMetadata = updateQueriedMetadata[K, M](queryKeyBounds, metadata)

    val decompose = (bounds: KeyBounds[K]) =>
      keyIndex.indexRanges(bounds).map { case (min, max) =>
        new AccumuloRange(new Text(AccumuloKeyEncoder.long2Bytes(min)), new Text(AccumuloKeyEncoder.long2Bytes(max)))
      }

    val seq = AccumuloCollectionReader.read[K, V](header.tileTable, columnFamily(id), queryKeyBounds, decompose, filterIndexOnly, Some(writerSchema))

    new ContextCollection(seq, layerMetadata)
  }
}

object AccumuloCollectionLayerReader {
  def apply(attributeStore: AccumuloAttributeStore)(implicit instance: AccumuloInstance): AccumuloCollectionLayerReader =
    new AccumuloCollectionLayerReader(attributeStore)

  def apply(implicit instance: AccumuloInstance): AccumuloCollectionLayerReader =
    new AccumuloCollectionLayerReader(AccumuloAttributeStore(instance.connector))
}
