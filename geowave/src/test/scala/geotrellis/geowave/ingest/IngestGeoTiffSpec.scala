/*
 * Copyright 2021 Azavea
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

import geotrellis.geowave.dsl._
import geotrellis.geowave.dsl.syntax._
import geotrellis.geowave.adapter.geotiff._
import geotrellis.geowave.adapter.geotiff.GeoTiffAdapter.GEOTIFF_TIME_FORMATTER_DEFAULT
import geotrellis.geowave.api.SQueryBuilder
import geotrellis.geowave.index.query.ExplicitSpatialTemporalElevationQuery
import geotrellis.raster.MultibandTile
import geotrellis.raster.io.geotiff.GeoTiff

import org.locationtech.geowave.core.geotime.store.query.{ExplicitSpatialQuery, ExplicitSpatialTemporalQuery}
import org.locationtech.geowave.core.store.CloseableIterator
import org.locationtech.geowave.core.index.sfc.data.NumericRange
import org.locationtech.geowave.core.geotime.store.dimension.Time.TimeRange
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter
import cats.syntax.option._
import geotrellis.vector._
import cats.Id

import java.net.URI
import java.time.ZonedDateTime
import java.util.Date

import geotrellis.geowave.TestEnvironment
import org.scalatest.matchers.should.Matchers
import cats.effect.IO

class IngestGeoTiffSpec extends TestEnvironment with Matchers {
  // TODO: runime should be configured
  import cats.effect.unsafe.implicits.global

  lazy val uri: String = "src/test/resources/raster/all-ones.tif"

  val ingestParameters = IngestParameters[TilingBounds](
    typeName     = "IngestMultibandGeoTiffSpec".typeName,
    dataType     = "GeoTiff".dataType,
    uri          = new URI(uri),
    options      = TilingBounds(depth = 1.some).some,
    namespace    = "IngestMultibandGeoTiffSpec".some
  )

  val indexParameters = IndexParameters(
    indices = IndexDefinition(
      indexName    = "IngestMultibandGeoTiffSpec".some,
      indexType    = "spatial_temporal_elevation".indexType,
      indexOptions = Map()
    ),
    typeName     = "IngestMultibandGeoTiffSpec".typeName,
    dataType     = "GeoTiff".dataType,
    namespace    = "IngestMultibandGeoTiffSpec".some
  )

  val date = ZonedDateTime.from(GEOTIFF_TIME_FORMATTER_DEFAULT.parse("2019:11:01 12:00:00")).toInstant

  val deleteParameters = DeleteParameters(
    typeName  = "IngestMultibandGeoTiffSpec".typeName,
    indexName = "IngestMultibandGeoTiffSpec",
    geometry  =
      """
        |{"type":"Polygon","coordinates":[[[141.7066666666667,-18.373333333333342],[141.7066666666667,-17.52000000000001],[142.56000000000003,-17.52000000000001],[142.56000000000003,-18.373333333333342],[141.7066666666667,-18.373333333333342]]]}
        |""".stripMargin.parseGeoJson[Polygon]().some,
    namespace = "IngestMultibandGeoTiffSpec".some,
    time      = new TimeRange(0, date.toEpochMilli).some,
    elevation     = new NumericRange(0d, 30000d).some,
    compareOp = BasicQueryFilter.BasicQueryCompareOperation.INTERSECTS
  )

  describe("IngestMultibandGeoTiffSpec spatial temporal elevation index spec") {
    val index = ConfigureIndex(indexParameters).head
    val indexName = indexParameters.indices.flatMap(_.indexName).head

    // default behavior is to read the whole file as a single tile
    val iterator = IngestGeoTiff.dataTypeReader[IO].read(new URI(uri), options=None).unsafeRunSync()
    val tiff = iterator.next()
    lazy val geowaveDataStore = ingestParameters.dataStore

    it("index name should be set as expected") {
      indexName shouldBe indexName
    }

    it("should write indexed GeoTiff") {
      IngestGeoTiff(ingestParameters)
    }

    it("should query the entire contents of a store") {
      val query =
        SQueryBuilder
          .newBuilder[GeoTiff[MultibandTile]]
          .addTypeName(indexParameters.typeName)
          .indexName(indexName)

      val it: CloseableIterator[GeoTiff[MultibandTile]] = geowaveDataStore.query(query.build())
      it.hasNext shouldBe true
      val actualTiff = it.next()
      actualTiff.toByteArray should contain theSameElementsAs tiff.toByteArray
      it.hasNext shouldBe false
    }

    it("should query store by extent") {
      val extent = tiff.extent
      val spatialQuery = new ExplicitSpatialQuery(extent.toPolygon())

      val query =
        SQueryBuilder
          .newBuilder[GeoTiff[MultibandTile]]
          .addTypeName(indexParameters.typeName)
          .indexName(indexName)
          .constraints(spatialQuery)

      val it: CloseableIterator[GeoTiff[MultibandTile]] = geowaveDataStore.query(query.build())
      it.hasNext shouldBe true
      val actualTiff = it.next()

      actualTiff.toByteArray should contain theSameElementsAs tiff.toByteArray
      it.hasNext shouldBe false
    }

    it("should query store by time") {
      val extent = tiff.extent
      val dateString = "2019:11:01 12:00:00"
      val date = Date.from(ZonedDateTime.from(GEOTIFF_TIME_FORMATTER_DEFAULT.parse(dateString)).toInstant)
      val spatialTemporalQuery = new ExplicitSpatialTemporalQuery(date, date, extent.toPolygon())

      val query =
        SQueryBuilder
          .newBuilder[GeoTiff[MultibandTile]]
          .addTypeName(indexParameters.typeName)
          .indexName(indexName)
          .constraints(spatialTemporalQuery)

      val it: CloseableIterator[GeoTiff[MultibandTile]] = geowaveDataStore.query(query.build())
      it.hasNext shouldBe true
      val actualTiff = it.next()

      actualTiff.toByteArray should contain theSameElementsAs tiff.toByteArray
      it.hasNext shouldBe false
    }

    it("should query store by elevation") {
      val extent = tiff.extent
      val dateString = "2019:11:01 12:00:00"
      val date = Date.from(ZonedDateTime.from(GEOTIFF_TIME_FORMATTER_DEFAULT.parse(dateString)).toInstant)
      val stdQuery = ExplicitSpatialTemporalElevationQuery(1, date, date, extent.toPolygon())

      val query =
        SQueryBuilder
          .newBuilder[GeoTiff[MultibandTile]]
          .addTypeName(indexParameters.typeName)
          .indexName(indexName)
          .constraints(stdQuery)

      val it: CloseableIterator[GeoTiff[MultibandTile]] = geowaveDataStore.query(query.build())
      it.hasNext shouldBe true
      val actualTiff = it.next()

      actualTiff.toByteArray should contain theSameElementsAs tiff.toByteArray
      it.hasNext shouldBe false
    }

    it("should return nothing querying out of the spatial tiff bounds") {
      val extent = Extent(0, 0, 1, 1)
      val dateString = "2019:11:01 12:00:00"
      val date = Date.from(ZonedDateTime.from(GEOTIFF_TIME_FORMATTER_DEFAULT.parse(dateString)).toInstant)
      val spatialTemporalQuery = new ExplicitSpatialTemporalQuery(date, date, extent.toPolygon())

      val query =
        SQueryBuilder
          .newBuilder[GeoTiff[MultibandTile]]
          .addTypeName(indexParameters.typeName)
          .indexName(indexName)
          .constraints(spatialTemporalQuery)

      val it: CloseableIterator[GeoTiff[MultibandTile]] = geowaveDataStore.query(query.build())
      it.hasNext shouldBe false
    }

    it("should return nothing querying out of the temporal bounds") {
      val extent = tiff.extent
      val dateString = "2017:11:01 12:00:00"
      val date = Date.from(ZonedDateTime.from(GEOTIFF_TIME_FORMATTER_DEFAULT.parse(dateString)).toInstant)
      val spatialTemporalQuery = new ExplicitSpatialTemporalQuery(date, date, extent.toPolygon())

      val query =
        SQueryBuilder
          .newBuilder[GeoTiff[MultibandTile]]
          .addTypeName(indexParameters.typeName)
          .indexName(indexName)
          .constraints(spatialTemporalQuery)

      val it: CloseableIterator[GeoTiff[MultibandTile]] = geowaveDataStore.query(query.build())
      it.hasNext shouldBe false
    }

    it("should query nothing out of the elevation bounds") {
      val extent = tiff.extent
      val dateString = "2019:11:01 12:00:00"
      val date = Date.from(ZonedDateTime.from(GEOTIFF_TIME_FORMATTER_DEFAULT.parse(dateString)).toInstant)
      val stdQuery = ExplicitSpatialTemporalElevationQuery(100, date, date, extent.toPolygon())

      val query =
        SQueryBuilder
          .newBuilder[GeoTiff[MultibandTile]]
          .addTypeName(indexParameters.typeName)
          .indexName(indexName)
          .constraints(stdQuery)

      val it: CloseableIterator[GeoTiff[MultibandTile]] = geowaveDataStore.query(query.build())
      it.hasNext shouldBe false
    }

    it("should return nothing querying out of the spatialtemporal bounds") {
      val extent = Extent(0, 0, 1, 1)
      val dateString = "2017:11:01 12:00:00"
      val date = Date.from(ZonedDateTime.from(GEOTIFF_TIME_FORMATTER_DEFAULT.parse(dateString)).toInstant)
      val spatialTemporalQuery = new ExplicitSpatialTemporalQuery(date, date, extent.toPolygon())

      val query =
        SQueryBuilder
          .newBuilder[GeoTiff[MultibandTile]]
          .addTypeName(indexParameters.typeName)
          .indexName(indexName)
          .constraints(spatialTemporalQuery)

      val it: CloseableIterator[GeoTiff[MultibandTile]] = geowaveDataStore.query(query.build())
      it.hasNext shouldBe false
    }

    it("should delete nothing by the query out of bounds") {
      val extent = Extent(0, 0, 1, 1)
      val dateString = "2017:11:01 12:00:00"
      val date = Date.from(ZonedDateTime.from(GEOTIFF_TIME_FORMATTER_DEFAULT.parse(dateString)).toInstant)
      val spatialTemporalQuery = new ExplicitSpatialTemporalQuery(date, date, extent.toPolygon())

      val dquery =
        SQueryBuilder
          .newBuilder[GeoTiff[MultibandTile]]
          .addTypeName(indexParameters.typeName)
          .indexName(indexName)
          .constraints(spatialTemporalQuery)

      geowaveDataStore.delete(dquery.build()) shouldBe true

      val query =
        SQueryBuilder
          .newBuilder[GeoTiff[MultibandTile]]
          .addTypeName(indexParameters.typeName)
          .indexName(indexName)

      val it: CloseableIterator[GeoTiff[MultibandTile]] = geowaveDataStore.query(query.build())
      it.hasNext shouldBe true
    }

    it("should delete everything by the query") {
      val extent = tiff.extent
      val dateString = "2019:11:01 12:00:00"
      val date = Date.from(ZonedDateTime.from(GEOTIFF_TIME_FORMATTER_DEFAULT.parse(dateString)).toInstant)
      val spatialTemporalQuery = new ExplicitSpatialTemporalQuery(date, date, extent.toPolygon())

      val query =
        SQueryBuilder
          .newBuilder[GeoTiff[MultibandTile]]
          .addTypeName(indexParameters.typeName)
          .indexName(indexName)
          .constraints(spatialTemporalQuery)

      geowaveDataStore.delete(query.build()) shouldBe true

      val it: CloseableIterator[GeoTiff[MultibandTile]] = geowaveDataStore.query(query.build())
      it.hasNext shouldBe false
    }

    it("should delete everything using the DeleteParameters") {
      // write it again
      IngestGeoTiff(ingestParameters)
      // execute the delete query
      ExecuteQuery.delete(deleteParameters)

      // should query nothing
      val query =
        SQueryBuilder
          .newBuilder[GeoTiff[MultibandTile]]
          .addTypeName(indexParameters.typeName)
          .indexName(indexName)

      val it: CloseableIterator[GeoTiff[MultibandTile]] = geowaveDataStore.query(query.build())
      it.hasNext shouldBe false
    }
  }
}
