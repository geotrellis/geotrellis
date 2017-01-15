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

package geotrellis.spark

import geotrellis.raster.{MultibandTile, Tile}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

package object streaming extends
  streaming.buffer.Implicits with
  streaming.crop.Implicits with
  streaming.filter.Implicits {

  type TileLayerDStream[K] = DStream[(K, Tile)] with Metadata[TileLayerMetadata[K]]
  object TileLayerDStream {
    def apply[K](stream: DStream[(K, Tile)], metadata: TileLayerMetadata[K])(implicit ssc: StreamingContext): TileLayerDStream[K] =
      new ContextDStream(stream, metadata)
  }

  type MultibandTileLayerDStream[K] = DStream[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]
  object MultibandTileLayerDStream {
    def apply[K](rdd: DStream[(K, MultibandTile)], metadata: TileLayerMetadata[K])(implicit ssc: StreamingContext): MultibandTileLayerDStream[K] =
      new ContextDStream(rdd, metadata)
  }


  implicit class WithStreamingContextWrapper[K, V, M](val stream: DStream[(K, V)] with Metadata[M]) {
    implicit val ssc = stream.context
    def withContext[K2, V2](f: DStream[(K, V)] => DStream[(K2, V2)]) =
      new ContextDStream(f(stream), stream.metadata)

    def mapContext[M2](f: M => M2) =
      new ContextDStream(stream, f(stream.metadata))

    def transformWithContext[K2, V2](f: RDD[(K, V)] with Metadata[M] => RDD[(K2, V2)] with Metadata[M]): ContextDStream[K2, V2, M] =
      new ContextDStream[K2, V2, M](stream.transform[(K2, V2)]{ rdd: RDD[(K, V)] => f(new ContextRDD(rdd, stream.metadata)) }, stream.metadata)(stream.context)
  }
}
