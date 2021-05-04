/*
 * Copyright 2020 Azavea
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

package geotrellis.geowave.ingest

import java.net.URI

import cats.effect.{IO, Sync}
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import geotrellis.geowave.dsl._
import geotrellis.raster.MultibandTile
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.{GeoTiff, MultibandGeoTiff}
import org.locationtech.geowave.core.store.api.WriteResults
import org.slf4j.LoggerFactory

object IngestGeoTiff {
  private val logger = LoggerFactory.getLogger(this.getClass())

  // TODO: runime should be configured
  import cats.effect.unsafe.implicits.global

  def apply(params: IngestParameters[TilingBounds]): List[WriteResults] =
    sync[IO](params).unsafeRunSync()

  def sync[F[_]: Sync](
    params: IngestParameters[TilingBounds]
  ): F[List[WriteResults]] = {
    logger.info(s"Reading: ${params.uri} as ${params.dataType} with ${params.options} options")
    val reader = dataTypeReader[F]
    val values = reader.read(params.uri, params.options)

    values.map { chunks =>
      val writer = params.writer[GeoTiff[MultibandTile]]
      if (writer == null) throw new RuntimeException(s"No writer for ${params.typeName}")

      try {
        val results = chunks.map(writer.write).toList
        logger.info(s"Wrote: ${params.uri} to ${params.typeName}")
        results
      }
      finally writer.close()
    }
  }

  def dataTypeReader[F[_]: Sync]: DataTypeReader[F, TilingBounds, GeoTiff[MultibandTile]] =
    new DataTypeReader[F, TilingBounds, GeoTiff[MultibandTile]] {
      def read(uri: URI, options: Option[TilingBounds]): F[Iterator[GeoTiff[MultibandTile]]] =
        metadata(uri) >>= {
          _
            .split(options.getOrElse(TilingBounds()))
            .map { md => readVoxelBounds(uri, md.bounds) }
            .toList
            .sequence
            .map(_.iterator.flatten)
        }

      private
      def readVoxelBounds(uri: URI, bounds: VoxelBounds3D): F[Iterator[GeoTiff[MultibandTile]]] = Sync[F].delay {
        val rs = GeoTiffRasterSource(uri.toString)
        rs
          .read(bounds.toGridBounds.toGridType[Long], bounds.depthMin to bounds.depthMax)
          .map(MultibandGeoTiff(_, rs.crs, rs.metadata.tags))
          .iterator
      }

      private
      def metadata(uri: URI): F[IngestGeoTiffMetadata] = Sync[F].delay {
        val rs = GeoTiffRasterSource(uri.toString)

        IngestGeoTiffMetadata(
          VoxelDimensions3D(rs.cols.toInt, rs.rows.toInt, rs.bandCount).toVoxelBounds,
          rs.extent,
          rs.metadata
        )
      }
    }
}
