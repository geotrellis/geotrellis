/*
 * Copyright 2019 Azavea
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

package geotrellis.spark.gdal

import geotrellis.layer._
import geotrellis.raster.gdal._
import geotrellis.raster.geotiff._
import geotrellis.raster.{RasterSource, ReadingSource}
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.spark._
import geotrellis.spark.store.hadoop._
import geotrellis.spark.testkit._
import geotrellis.store.hadoop._

import cats.effect.IO
import cats.implicits._
import spire.syntax.cfor._
import org.apache.spark.rdd.RDD

import org.scalatest.Inspectors._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

class GDALRasterSourceRDDSpec extends AnyFunSpec with TestEnvironment with BeforeAndAfterAll {
  import geotrellis.GDALTestUtils._
  // TODO: runime should be configured
  import cats.effect.unsafe.implicits.global

  val uri = gdalGeoTiffPath("vlm/aspect-tiled.tif")
  def filePathByIndex(i: Int): String = sparkGeoTiffPath(s"vlm/aspect-tiled-$i.tif")
  lazy val rasterSource = GeoTiffRasterSource(uri)
  val targetCRS = CRS.fromEpsgCode(3857)
  val scheme = ZoomedLayoutScheme(targetCRS)
  lazy val layout = scheme.levelForZoom(13).layout

  lazy val reprojectedSource = rasterSource.reprojectToGrid(targetCRS, layout)

  describe("reading in GeoTiffs as RDDs") {

    it("should have the right number of tiles") {
      val expectedKeys =
        layout
          .mapTransform
          .keysForGeometry(reprojectedSource.extent.toPolygon())
          .toSeq
          .sortBy { key => (key.col, key.row) }

      info(s"RasterSource CRS: ${reprojectedSource.crs}")

      val rdd = RasterSourceRDD.spatial(reprojectedSource, layout)

      val actualKeys = rdd.keys.collect().sortBy { key => (key.col, key.row) }

      for ((actual, expected) <- actualKeys.zip(expectedKeys)) {
        actual should be(expected)
      }
    }

    it("should read in the tiles as squares") {
      val reprojectedRasterSource = rasterSource.reprojectToGrid(targetCRS, layout)
      val rdd = RasterSourceRDD.spatial(reprojectedRasterSource, layout)
      val rows = rdd.collect()

      forAll(rows) { case (key, tile) =>
        withClue(s"$key") {
          tile should have(
            // dimensions(256, 256),
            cellType(rasterSource.cellType),
            bandCount(rasterSource.bandCount)
          )
        }
      }
    }
  }

  describe("Match reprojection from HadoopGeoTiffRDD") {
    val floatingLayout = FloatingLayoutScheme(256)
    val geoTiffRDD = HadoopGeoTiffRDD.spatialMultiband(uri)
    val md = geoTiffRDD.collectMetadata[SpatialKey](floatingLayout)._2

    val reprojectedExpectedRDD: MultibandTileLayerRDD[SpatialKey] =
      geoTiffRDD.tileToLayout(md).reproject(targetCRS, layout)._2.persist()

    def assertRDDLayersEqual(
      expected: MultibandTileLayerRDD[SpatialKey],
      actual: MultibandTileLayerRDD[SpatialKey],
      matchRasters: Boolean = false
    ): Unit = {
      val joinedRDD = expected.filter { case (_, t) => !t.band(0).isNoDataTile }.leftOuterJoin(actual)

      joinedRDD.collect().foreach { case (key, (expected, actualTile)) =>
        actualTile match {
          case Some(actual) =>
            /*writePngOutputTile(
              actual,
              name = "actual",
              discriminator = s"-${key}"
            )

            writePngOutputTile(
              expected,
              name = "expected",
              discriminator = s"-${key}"
            )*/

            // withGeoTiffClue(key, layout, actual, expected, targetCRS) {
            withClue(s"$key:") {
              expected.dimensions should be(actual.dimensions)
              if (matchRasters) assertTilesEqual(expected, actual)
            }
          // }

          case None =>
            throw new Exception(s"$key does not exist in the rasterSourceRDD")
        }
      }
    }

    it("should reproduce tileToLayout") {
      // This should be the same as result of .tileToLayout(md.layout)
      val rasterSourceRDD: MultibandTileLayerRDD[SpatialKey] = RasterSourceRDD.spatial(rasterSource, md.layout)

      // Complete the reprojection
      val reprojectedSource = rasterSourceRDD.reproject(targetCRS, layout)._2

      assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSource)
    }

    it("should reproduce tileToLayout followed by reproject") {
      val reprojectedRasterSource = rasterSource.reprojectToGrid(targetCRS, layout)
      val reprojectedSourceRDD: MultibandTileLayerRDD[SpatialKey] =
        RasterSourceRDD.spatial(Seq(reprojectedRasterSource), layout)

      // geotrellis.raster.io.geotiff.GeoTiff(reprojectedExpectedRDD.stitch, targetCRS).write("/tmp/expected.tif")
      // geotrellis.raster.io.geotiff.GeoTiff(reprojectedSourceRDD.stitch, targetCRS).write("/tmp/actual.tif")

      val actual = reprojectedSourceRDD.stitch().tile.band(0)
      val expected = reprojectedExpectedRDD.stitch().tile.band(0)

      var (diff, pixels, mismatched) = (0d, 0d, 0)
      cfor(0)(_ < math.min(actual.cols, expected.cols), _ + 1) { c =>
        cfor(0)(_ < math.min(actual.rows, expected.rows), _ + 1) { r =>
          pixels += 1d
          if (math.abs(actual.get(c, r) - expected.get(c, r)) > 1e-6)
            diff += 1d
          if (isNoData(actual.get(c, r)) != isNoData(expected.get(c, r)))
            mismatched += 1
        }
      }

      assert(diff / pixels < 0.005) // half percent of pixels or less are not equal
      assert(mismatched < 3)
    }

    it("should reproduce tileToLayout when given an RDD[RasterSource]") {
      val rasterSourceRDD: RDD[RasterSource] = sc.parallelize(Seq(rasterSource))

      // Need to define these here or else a serialization error will occur
      val targetLayout = layout
      val crs = targetCRS

      val reprojectedRasterSourceRDD: RDD[RasterSource] = rasterSourceRDD.map { _.reprojectToGrid(crs, targetLayout) }

      val tiledSource: MultibandTileLayerRDD[SpatialKey] = RasterSourceRDD.tiledLayerRDD(reprojectedRasterSourceRDD, targetLayout)

      assertRDDLayersEqual(reprojectedExpectedRDD, tiledSource)
    }

    describe("GDALRasterSource") {
      val expectedFilePath = gdalGeoTiffPath("vlm/aspect-tiled-near-merc-rdd.tif")

      it("should reproduce tileToLayout") {
        val rasterSource = GDALRasterSource(uri)

        // This should be the same as result of .tileToLayout(md.layout)
        val rasterSourceRDD: MultibandTileLayerRDD[SpatialKey] = RasterSourceRDD.spatial(rasterSource, md.layout)
        // Complete the reprojection
        val reprojectedSource = rasterSourceRDD.reproject(targetCRS, layout)._2

        assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSource, true)
      }

      it("should reproduce tileToLayout followed by reproject GDAL") {
        val expectedRasterSource = GDALRasterSource(expectedFilePath)
        val reprojectedExpectedRDDGDAL: MultibandTileLayerRDD[SpatialKey] = RasterSourceRDD.spatial(expectedRasterSource, layout)
        val rasterSource = GDALRasterSource(uri)
        val reprojectedRasterSource = rasterSource.reprojectToGrid(targetCRS, layout)

        // This should be the same as .tileToLayout(md.layout).reproject(crs, layout)
        val reprojectedSourceRDD: MultibandTileLayerRDD[SpatialKey] = RasterSourceRDD.spatial(reprojectedRasterSource, layout)

        assertRDDLayersEqual(reprojectedExpectedRDDGDAL, reprojectedSourceRDD, true)
      }

      def parallelSpec(n: Int = 1000): List[RasterSource] = {
        println(java.lang.Thread.activeCount())

        /** Functions to trigger Datasets computation */
        def ltsWithDatasetsTriggered(lts: LayoutTileSource[SpatialKey]): LayoutTileSource[SpatialKey] = { rsWithDatasetsTriggered(lts.source); lts }
        def rsWithDatasetsTriggered(rs: RasterSource): RasterSource = {
          val brs = rs.asInstanceOf[GDALRasterSource]
          brs.dataset.rasterExtent
          brs.dataset.rasterExtent(GDALDataset.SOURCE)
          rs
        }

        /** Do smth usual with the original RasterSource to force VRTs allocation */
        def reprojRS(i: Int): LayoutTileSource[SpatialKey] =
          ltsWithDatasetsTriggered(
            rsWithDatasetsTriggered(
              rsWithDatasetsTriggered(GDALRasterSource(filePathByIndex(i)))
                .reprojectToGrid(targetCRS, layout)
            ).tileToLayout(layout)
          )

        /** Simulate possible RF backsplash calls */
        def dirtyCalls(rs: RasterSource): RasterSource = {
          val ds = rs.asInstanceOf[GDALRasterSource].dataset
          ds.rasterExtent
          ds.crs
          ds.cellSize
          ds.extent
          rs
        }

        val res = (1 to n).toList.flatMap { _ =>
          (0 to 4).flatMap { i =>
            List(IO {
              // println(Thread.currentThread().getName())
              // Thread.sleep((Math.random() * 100).toLong)
              val lts = reprojRS(i)
              lts.readAll(lts.keys.take(10).iterator)
              reprojRS(i).source.resolutions

              dirtyCalls(reprojRS(i).source)
            }, IO {
              // println(Thread.currentThread().getName())
              // Thread.sleep((Math.random() * 100).toLong)
              val lts = reprojRS(i)
              lts.readAll(lts.keys.take(10).iterator)
              reprojRS(i).source.resolutions

              dirtyCalls(reprojRS(i).source)
            }, IO {
              // println(Thread.currentThread().getName())
              // Thread.sleep((Math.random() * 100).toLong)
              val lts = reprojRS(i)
              lts.readAll(lts.keys.take(10).iterator)
              reprojRS(i).source.resolutions

              dirtyCalls(reprojRS(i).source)
            })
          }
        }.parSequence.unsafeRunSync()

        println(java.lang.Thread.activeCount())

        res
      }

      it("should not fail on parallelization with a fork join pool") {
        val i = 1000

        parallelSpec(i)
      }

      it("should not fail on parallelization with a fixed thread pool") {
        val i = 1000
        val n = 100
        val pool = Executors.newFixedThreadPool(n)
        val ec = ExecutionContext.fromExecutor(pool)

        parallelSpec(i)

        pool.shutdown()
      }
    }
  }

  describe("RasterSourceRDD.read") {

    val floatingScheme = FloatingLayoutScheme(500, 270)
    val floatingLayout = floatingScheme.levelFor(rasterSource.extent, rasterSource.cellSize).layout

    val cellType = rasterSource.cellType

    val multibandTilePath = sparkGeoTiffPath("vlm/aspect-tiled-0-1-2.tif")

    val noDataTile = ArrayTile.alloc(cellType, rasterSource.cols.toInt, rasterSource.rows.toInt).fill(NODATA).interpretAs(cellType)

    val paths: Seq[String] = 0 to 5 map { index => filePathByIndex(index) }

    lazy val expectedMultibandTile = {
      val tiles = paths.map { MultibandGeoTiff(_, streaming = false).tile.band(0) }
      MultibandTile(tiles)
    }

    it("should read in singleband tiles") {
      val readingSources: Seq[ReadingSource] = paths.zipWithIndex.map { case (path, index) => ReadingSource(GeoTiffRasterSource(path), 0, index) }

      val expected = expectedMultibandTile
      val actual = RasterSourceRDD.read(readingSources, floatingLayout).stitch()

      // random chip to test agains, to speed up tests
      val gridBounds = actual.rasterExtent.gridBoundsFor(randomExtentWithin(actual.extent))

      expected.dimensions shouldBe actual.dimensions

      assertEqual(expected.crop(gridBounds), actual.tile.crop(gridBounds))
    }

    it("should read in singleband tiles with missing bands") {
      val readingSources: Seq[ReadingSource] =
        Seq(
          ReadingSource(GeoTiffRasterSource(paths(0)), 0, 0),
          ReadingSource(GeoTiffRasterSource(paths(2)), 0, 1),
          ReadingSource(GeoTiffRasterSource(paths(4)), 0, 3)
        )

      val expected =
        MultibandTile(
          expectedMultibandTile.band(0),
          expectedMultibandTile.band(2),
          noDataTile,
          expectedMultibandTile.band(4)
        )

      val actual = RasterSourceRDD.read(readingSources, floatingLayout).stitch()

      // random chip to test agains, to speed up tests
      val gridBounds = actual.rasterExtent.gridBoundsFor(randomExtentWithin(actual.extent))

      expected.dimensions shouldBe actual.dimensions

      assertEqual(expected.crop(gridBounds), actual.tile.crop(gridBounds))
    }

    it("should read in singleband and multiband tiles") {
      val readingSources: Seq[ReadingSource] =
        Seq(
          ReadingSource(GeoTiffRasterSource(multibandTilePath), 0, 0),
          ReadingSource(GeoTiffRasterSource(paths(1)), 0, 1),
          ReadingSource(GeoTiffRasterSource(multibandTilePath), 2, 2),
          ReadingSource(GeoTiffRasterSource(paths(3)), 0, 3),
          ReadingSource(GeoTiffRasterSource(paths(4)), 0, 4),
          ReadingSource(GeoTiffRasterSource(paths(5)), 0, 5)
        )

      val expected = expectedMultibandTile
      val actual = RasterSourceRDD.read(readingSources, floatingLayout).stitch()

      // random chip to test agains, to speed up tests
      val gridBounds = actual.rasterExtent.gridBoundsFor(randomExtentWithin(actual.extent))

      expected.dimensions shouldBe actual.dimensions

      assertEqual(expected.crop(gridBounds), actual.tile.crop(gridBounds))
    }

    it("should read in singleband and multiband tiles with missing bands") {
      val readingSources: Seq[ReadingSource] =
        Seq(
          ReadingSource(GeoTiffRasterSource(paths(4)), 0, 5),
          ReadingSource(GeoTiffRasterSource(multibandTilePath), 1, 0),
          ReadingSource(GeoTiffRasterSource(multibandTilePath), 2, 1)
        )

      val expected =
        MultibandTile(
          expectedMultibandTile.band(1),
          expectedMultibandTile.band(2),
          noDataTile,
          noDataTile,
          noDataTile,
          expectedMultibandTile.band(4)
        )

      val actual = RasterSourceRDD.read(readingSources, floatingLayout).stitch()

      // random chip to test agains, to speed up tests
      val gridBounds = actual.rasterExtent.gridBoundsFor(randomExtentWithin(actual.extent))

      expected.dimensions shouldBe actual.dimensions

      assertEqual(expected.crop(gridBounds), actual.tile.crop(gridBounds))
    }
  }
}
