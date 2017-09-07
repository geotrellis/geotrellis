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

import geotrellis.spark._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.json._
import geotrellis.spark.merge._
import geotrellis.util._

import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import spray.json._

import scala.reflect.ClassTag

abstract class LayerUpdater[ID] {

  // protected def _overwrite[
  //   K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
  //   V: AvroRecordCodec: ClassTag,
  //   M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  // ](id: ID, rdd: RDD[(K, V)] with Metadata[M], keyBounds: KeyBounds[K]): Unit

  // protected def _update[
  //   K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
  //   V: AvroRecordCodec: ClassTag,
  //   M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  // ](id: ID, rdd: RDD[(K, V)] with Metadata[M], keyBounds: KeyBounds[K], mergeFunc: (V, V) => V): Unit

  protected def schemaHasChanged[K: AvroRecordCodec, V: AvroRecordCodec](writerSchema: Schema): Boolean = {
    val codec  = KeyValueRecordCodec[K, V]
    val schema = codec.schema
    !schema.fingerprintMatches(writerSchema)
  }

  def update[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](id: ID, rdd: RDD[(K, V)] with Metadata[M], mergeFunc: (V, V) => V): Unit
  // =
  //   rdd.metadata.getComponent[Bounds[K]] match {
  //     case keyBounds: KeyBounds[K] =>
  //       _update(id, rdd, keyBounds, mergeFunc)
  //     case EmptyBounds =>
  //       throw new EmptyBoundsError(s"Cannot update layer $id with a layer with empty bounds.")
  //   }

  def update[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](id: ID, rdd: RDD[(K, V)] with Metadata[M]): Unit
  // =
  //   rdd.metadata.getComponent[Bounds[K]] match {
  //     case keyBounds: KeyBounds[K] =>
  //       // By default, we want the updating tile to replace the existing tile.
  //       val mergeFunc: (V, V) => V = { (existing, updating) => updating }
  //       _update(id, rdd, keyBounds, mergeFunc)
  //     case EmptyBounds =>
  //       throw new EmptyBoundsError(s"Cannot update layer $id with a layer with empty bounds.")
  //   }

  def overwrite[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]: Mergable
  ](id: ID, rdd: RDD[(K, V)] with Metadata[M]): Unit
  // =
  //   rdd.metadata.getComponent[Bounds[K]] match {
  //     case keyBounds: KeyBounds[K] =>
  //       _overwrite(id, rdd, keyBounds)
  //     case EmptyBounds =>
  //       throw new EmptyBoundsError(s"Cannot update layer $id with a layer with empty bounds.")
  //   }
}
