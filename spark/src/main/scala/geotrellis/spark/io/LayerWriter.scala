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

package geotrellis.spark.io

import geotrellis.spark._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.index._
import geotrellis.spark.io.json._
import geotrellis.spark.merge._
import geotrellis.util._

import org.apache.avro.Schema
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import spray.json._

import scala.reflect.ClassTag

import java.util.ServiceLoader
import java.net.URI


trait LayerWriter[ID] {
  val attributeStore: AttributeStore

  // Layer Updating
  protected def _overwrite[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](sc: SparkContext, id: ID, rdd: RDD[(K, V)] with Metadata[M], keyBounds: KeyBounds[K]): Unit

  protected def _update[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](sc: SparkContext, id: ID, rdd: RDD[(K, V)] with Metadata[M], keyBounds: KeyBounds[K], mergeFunc: (V, V) => V): Unit

  protected def schemaHasChanged[K: AvroRecordCodec, V: AvroRecordCodec](writerSchema: Schema): Boolean = {
    val codec  = KeyValueRecordCodec[K, V]
    val schema = codec.schema
    !schema.fingerprintMatches(writerSchema)
  }

  def update[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](id: ID, rdd: RDD[(K, V)] with Metadata[M], mergeFunc: (V, V) => V)(implicit sc: SparkContext): Unit =
    rdd.metadata.getComponent[Bounds[K]] match {
      case keyBounds: KeyBounds[K] =>
        _update(sc, id, rdd, keyBounds, mergeFunc)
      case EmptyBounds =>
        throw new EmptyBoundsError(s"Cannot update layer $id with a layer with empty bounds.")
    }

  def update[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](id: ID, rdd: RDD[(K, V)] with Metadata[M])(implicit sc: SparkContext): Unit =
    rdd.metadata.getComponent[Bounds[K]] match {
      case keyBounds: KeyBounds[K] =>
        // By default, we want the updating tile to replace the existing tile.
        val mergeFunc: (V, V) => V = { (existing, updating) => updating }
        _update(sc, id, rdd, keyBounds, mergeFunc)
      case EmptyBounds =>
        throw new EmptyBoundsError(s"Cannot update layer $id with a layer with empty bounds.")
    }

  def overwrite[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](id: ID, rdd: RDD[(K, V)] with Metadata[M])(implicit sc: SparkContext): Unit =
    rdd.metadata.getComponent[Bounds[K]] match {
      case keyBounds: KeyBounds[K] =>
        _overwrite(sc, id, rdd, keyBounds)
      case EmptyBounds =>
        throw new EmptyBoundsError(s"Cannot update layer $id with a layer with empty bounds.")
    }

  // Layer Writing
  protected def _write[
    K: AvroRecordCodec: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]
  ](id: ID, layer: RDD[(K, V)] with Metadata[M], keyIndex: KeyIndex[K]): Unit

  def write[
    K: AvroRecordCodec: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]
  ](id: ID, layer: RDD[(K, V)] with Metadata[M], keyIndex: KeyIndex[K]): Unit =
    layer.metadata.getComponent[Bounds[K]] match {
      case keyBounds: KeyBounds[K] =>
        _write[K, V, M](id, layer, keyIndex)
      case EmptyBounds =>
        throw new EmptyBoundsError("Cannot write layer with empty bounds.")
    }

  def write[
    K: AvroRecordCodec: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]
  ](id: ID, layer: RDD[(K, V)] with Metadata[M], keyIndexMethod: KeyIndexMethod[K]): Unit =
    layer.metadata.getComponent[Bounds[K]] match {
      case keyBounds: KeyBounds[K] =>
        val keyIndex = keyIndexMethod.createIndex(keyBounds)
        _write[K, V, M](id, layer, keyIndex)
      case EmptyBounds =>
        throw new EmptyBoundsError("Cannot write layer with empty bounds.")
    }

  def writer[
    K: AvroRecordCodec: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]
  ](keyIndexMethod: KeyIndexMethod[K]):  Writer[ID, RDD[(K, V)] with Metadata[M]] =
    new Writer[ID, RDD[(K, V)] with Metadata[M]] {
      def write(id: ID, layer: RDD[(K, V)] with Metadata[M]) =
        LayerWriter.this.write[K, V, M](id, layer, keyIndexMethod)
    }

  def writer[
    K: AvroRecordCodec: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]
  ](keyIndex: KeyIndex[K]):  Writer[ID, RDD[(K, V)] with Metadata[M]] =
    new Writer[ID, RDD[(K, V)] with Metadata[M]] {
      def write(id: ID, layer: RDD[(K, V)] with Metadata[M]) =
        LayerWriter.this.write[K, V, M](id, layer, keyIndex)
    }
}

object LayerWriter {
  /**
   * Produce LayerWriter instance based on URI description.
   * Find instances of [[LayerWriterProvider]] through Java SPI.
   */
  def apply(attributeStore: AttributeStore, layerWriterUri: URI): LayerWriter[LayerId] = {
    import scala.collection.JavaConversions._
    ServiceLoader.load(classOf[LayerWriterProvider]).iterator()
      .find(_.canProcess(layerWriterUri))
      .getOrElse(throw new RuntimeException(s"Unable to find LayerWriterProvider for $layerWriterUri"))
      .layerWriter(layerWriterUri, attributeStore)
  }

  /**
   * Produce LayerReader instance based on URI description.
   * Find instances of [[LayerWriterProvider]] through Java SPI.
   */
  def apply(attributeStoreUri: URI, layerWriterUri: URI): LayerWriter[LayerId] =
    apply(attributeStore = AttributeStore(attributeStoreUri), layerWriterUri)

  /**
   * Produce LayerReader instance based on URI description.
   * Find instances of [[LayerWriterProvider]] through Java SPI.
   * Required [[AttributeStoreProvider]] instance will be found from the same URI.
   */
  def apply(uri: URI): LayerWriter[LayerId] =
    apply(attributeStoreUri = uri, layerWriterUri = uri)

  def apply(attributeStore: AttributeStore, layerWriterUri: String): LayerWriter[LayerId] =
    apply(attributeStore, new URI(layerWriterUri))

  def apply(attributeStoreUri: String, layerWriterUri: String): LayerWriter[LayerId] =
    apply(new URI(attributeStoreUri), new URI(layerWriterUri))

  def apply(uri: String): LayerWriter[LayerId] =
    apply(new URI(uri))
}
