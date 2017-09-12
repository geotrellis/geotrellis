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

package geotrellis.spark.io.s3

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.index._
import geotrellis.spark.merge._
import geotrellis.util._

import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import spray.json._

import scala.reflect._

class S3LayerUpdater(
  val attributeStore: AttributeStore,
  layerReader: S3LayerReader
) extends LayerUpdater[LayerId] with LazyLogging {

  def rddWriter: S3RDDWriter = S3RDDWriter

  private def schemaHasChanged[K: AvroRecordCodec, V: AvroRecordCodec](writerSchema: Schema): Boolean = {
    val codec  = KeyValueRecordCodec[K, V]
    val schema = codec.schema
    !schema.fingerprintMatches(writerSchema)
  }

  def update[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](id: LayerId, rdd: RDD[(K, V)] with Metadata[M], mergeFunc: (V, V) => V): Unit =
    rdd.metadata.getComponent[Bounds[K]] match {
      case keyBounds: KeyBounds[K] =>
        _update(id, rdd, keyBounds, mergeFunc)
      case EmptyBounds =>
        throw new EmptyBoundsError(s"Cannot update layer $id with a layer with empty bounds.")
    }

  def update[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](id: LayerId, rdd: RDD[(K, V)] with Metadata[M]): Unit =
    rdd.metadata.getComponent[Bounds[K]] match {
      case keyBounds: KeyBounds[K] =>
        // By default, we want the updating tile to replace the existing tile.
        val mergeFunc: (V, V) => V = { (existing, updating) => updating }
        _update(id, rdd, keyBounds, mergeFunc)
      case EmptyBounds =>
        throw new EmptyBoundsError(s"Cannot update layer $id with a layer with empty bounds.")
    }

  def overwrite[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](id: LayerId, rdd: RDD[(K, V)] with Metadata[M]): Unit =
    rdd.metadata.getComponent[Bounds[K]] match {
      case keyBounds: KeyBounds[K] =>
        _overwrite(id, rdd, keyBounds)
      case EmptyBounds =>
        throw new EmptyBoundsError(s"Cannot update layer $id with a layer with empty bounds.")
    }

  protected def _overwrite[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](
    id: LayerId,
    rdd: RDD[(K, V)] with Metadata[M],
    keyBounds: KeyBounds[K]
  ): Unit = {
    _update(id, rdd, keyBounds, None)
  }

  protected def _update[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](
    id: LayerId,
    rdd: RDD[(K, V)] with Metadata[M],
    keyBounds: KeyBounds[K],
    mergeFunc: (V, V) => V
  ): Unit = {
    _update(id, rdd, keyBounds, Some(mergeFunc))
  }

  def _update[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](
    id: LayerId,
    rdd: RDD[(K, V)] with Metadata[M],
    keyBounds: KeyBounds[K],
    mergeFunc: Option[(V, V) => V]
  ) = {
    if (!attributeStore.layerExists(id)) throw new LayerNotFoundError(id)

    val LayerAttributes(header, metadata, keyIndex, writerSchema) = try {
      attributeStore.readLayerAttributes[S3LayerHeader, M, K](id)
    } catch {
      case e: AttributeNotFoundError => throw new LayerUpdateError(id).initCause(e)
    }

    if (!(keyIndex.keyBounds contains keyBounds))
      throw new LayerOutOfKeyBoundsError(id, keyIndex.keyBounds)

    val prefix = header.key
    val bucket = header.bucket

    val maxWidth = Index.digits(keyIndex.toIndex(keyIndex.keyBounds.maxKey))
    val keyPath = (key: K) => makePath(prefix, Index.encode(keyIndex.toIndex(key), maxWidth))

    logger.info(s"Saving updated RDD for layer ${id} to $bucket $prefix")
    val existingTiles =
      if(schemaHasChanged[K, V](writerSchema)) {
        logger.warn(s"RDD schema has changed, this requires rewriting the entire layer.")
        layerReader
          .read[K, V, M](id)

      } else {
        val query =
          new LayerQuery[K, M]
            .where(Intersects(rdd.metadata.getComponent[Bounds[K]].get))

        layerReader.read[K, V, M](id, query, layerReader.defaultNumPartitions, filterIndexOnly = true)
      }

    val updatedMetadata: M =
      metadata.merge(rdd.metadata)

    val updatedRdd: RDD[(K, V)] =
      mergeFunc match {
        case Some(mergeFunc) =>
          existingTiles
            .fullOuterJoin(rdd)
            .flatMapValues {
            case (Some(layerTile), Some(updateTile)) => Some(mergeFunc(layerTile, updateTile))
            case (Some(layerTile), _) => Some(layerTile)
            case (_, Some(updateTile)) => Some(updateTile)
            case _ => None
          }
        case None => rdd
      }

    val codec  = KeyValueRecordCodec[K, V]
    val schema = codec.schema

    // Write updated metadata, and the possibly updated schema
    // Only really need to write the metadata and schema
    attributeStore.writeLayerAttributes(id, header, updatedMetadata, keyIndex, schema)
    rddWriter.write(updatedRdd, bucket, keyPath)
  }
}

object S3LayerUpdater {
  def apply(
      bucket: String,
      prefix: String
  )(implicit sc: SparkContext): S3LayerUpdater =
    new S3LayerUpdater(
      S3AttributeStore(bucket, prefix),
      S3LayerReader(bucket, prefix)
    )
}
