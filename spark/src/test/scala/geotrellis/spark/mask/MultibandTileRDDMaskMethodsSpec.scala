package geotrellis.spark.mask

import geotrellis.raster._
import geotrellis.raster.testkit._
import geotrellis.spark._
import geotrellis.spark.testfiles._
import geotrellis.spark.testkit._
import geotrellis.vector._

import org.scalatest._

import scala.util.Random

class MultibandTileRDDMaskMethodsSpec extends FunSpec
    with Matchers
    with TestEnvironment
    with TestFiles
    with TileLayerRDDBuilders
    with RasterMatchers {
  describe("MultibandTileLayerRDDMask") {
    it("should properly mask a float32 user defined nodata layer") {
      val arr = (1 to (6*4)).map(_.toFloat).toArray
      arr(17) = -1.1f
      val sourceTile = FloatArrayTile(arr, 6, 4, FloatUserDefinedNoDataCellType(-1.1f))
      val tiles = MultibandTile(sourceTile, sourceTile, sourceTile, sourceTile)

      val layer = createMultibandTileLayerRDD(tiles, TileLayout(2, 2, 3, 2))

      val Extent(xmin, ymin, xmax, ymax) = layer.metadata.extent
      val dx = (xmax - xmin) / 3
      val dy = (ymax - ymin) / 2
      val mask = Extent(xmin + dx, ymin, xmax, ymin + dy)

      val n = -1.1f
      assertEqual(layer.mask(mask.toPolygon).stitch.tile.band(0),
        FloatArrayTile(
          Array(
            n, n, 15, 16, 17, n,
            n, n, 21, 22, 23, 24
          ), 6, 2, FloatUserDefinedNoDataCellType(-1.1f)
        )
      )
    }

    it("should properly mask a float32 user defined nodata layer, without filtering empty tiles") {
      val arr = (1 to (6*4)).map(_.toFloat).toArray
      arr(17) = -1.1f
      val sourceTile = FloatArrayTile(arr, 6, 4, FloatUserDefinedNoDataCellType(-1.1f))
      val tiles = MultibandTile(sourceTile, sourceTile, sourceTile, sourceTile)

      val layer = createMultibandTileLayerRDD(tiles, TileLayout(2, 2, 3, 2))

      val Extent(xmin, ymin, xmax, ymax) = layer.metadata.extent
      val dx = (xmax - xmin) / 3
      val dy = (ymax - ymin) / 2
      val mask = Extent(xmin + dx, ymin, xmax, ymin + dy)

      val n = -1.1f
      assertEqual(layer.mask(mask.toPolygon, options = Mask.Options(filterEmptyTiles = false)).stitch.tile.band(0),
        FloatArrayTile(
          Array(
            n, n, n, n, n, n,
            n, n, n, n, n, n,
            n, n, 15, 16, 17, n,
            n, n, 21, 22, 23, 24
          ), 6, 4, FloatUserDefinedNoDataCellType(-1.1f)
        )
      )
    }
  }

  describe("masking against more polygons") {
    val ones = AllOnesTestFile
    val rdd = ContextRDD(ones.mapValues { tile => MultibandTile(tile, tile, tile) }, ones.metadata)
    val tile = rdd.stitch.tile.band(0)
    val worldExt = rdd.metadata.extent
    val height = worldExt.height.toInt
    val width = worldExt.width.toInt

    def inMirror(bound: Int): Int = inRange(-bound to bound)
    def inRange(bounds: Range): Int = Random.nextInt(bounds.max - bounds.min) + bounds.min

    def triangle(size: Int, dx: Int, dy: Int): Line =
      Line(Seq[(Double, Double)]((0, 0), (size, 0), (size, size), (0, 0))
           .map { case (x, y) => (x + dx, y + dy) })

    def randomPolygons(number: Int = 50)(maxWidth: Int, maxHeight: Int): Seq[Polygon] = {
      val max = Math.min(maxWidth, maxHeight)
      val min = max / 10
      for {
        _ <- 1 to number
        size = inRange(min to max)
        placeLeft = Math.max(0, max - size)
        dx = inMirror(placeLeft) - size / 2
        dy = inMirror(placeLeft) - size / 2
        border = triangle(size, dx, dy)
        hole = triangle(size / 3, dx + size / 2, dy + size / 3)
      } yield Polygon(border, hole)
    }

    val opts = Mask.Options(filterEmptyTiles = false)

    it ("should be masked by random polygons") {
      randomPolygons()(width, height) foreach { poly =>
        if(poly.isValid) {
          val masked = rdd.mask(poly, options = opts).stitch.tile.band(0)
          val expected = tile.mask(worldExt, poly)
          masked.toArray() shouldEqual expected.toArray()
        }
      }
    }

    it ("should be masked by complex polygons") {
      val cases = Seq(
        Polygon(Line((-5, -16), (44, -16), (44, 33), (-5, -16)), Line((19, 0), (35, 0), (35, 16), (19, 0))),
        Polygon(Line((-84, -41), (40, -41), (40, 83), (-84, -41)), Line((-22, 0), (19, 0), (19, 41), (-22, 0))),
        Polygon(Line((-7, 0), (28, 0), (28, 35), (-7, 0)), Line((10, 11), (21, 11), (21, 22), (10, 11)))
      )
      cases foreach { poly =>
        val masked = rdd.mask(poly, options = opts).stitch.tile.band(0)
        val expected = tile.mask(worldExt, poly)
        masked.toArray() shouldEqual expected.toArray()
      }
    }

    it ("should be masked by random multipolygons") {
      val polygons = randomPolygons()(width, height)
      val multipolygons = polygons.zip(polygons.reverse).map { case (a, b) =>
        MultiPolygon(a, b)
      }
      multipolygons foreach { multipoly =>
        if(multipoly.isValid) {
          val masked = rdd.mask(multipoly, options = opts).stitch.tile.band(0)
          val expected = tile.mask(worldExt, multipoly)
          masked.toArray() shouldEqual expected.toArray()
        }
      }
    }

    it ("should be masked by complex multipolygons") {
      val cases = Seq(
        MultiPolygon(Polygon(Line((29, 15), (110, 15), (110, 96), (29, 15)), Line((69, 42), (96, 42), (96, 69), (69, 42))),
          Polygon(Line((-77, -78), (46, -78), (46, 45), (-77, -78)), Line((-16, -37), (25, -37), (25, 4), (-16, -37)))),
        MultiPolygon(Polygon(Line((-41, -17), (0, -17), (0, 24), (-41, -17)), Line((-21, -4), (-8, -4), (-8, 9), (-21, -4))),
          Polygon(Line((-83, -76), (-13, -76), (-13, -6), (-83, -76)), Line((-48, -53), (-25, -53), (-25, -30), (-48, -53))))
      )
      cases foreach { multipoly =>
        val masked = rdd.mask(multipoly, options = opts).stitch.tile.band(0)
        val expected = tile.mask(worldExt, multipoly)
        masked.toArray() shouldEqual expected.toArray()
      }
    }

    it ("should be masked by random extents") {
      val extents = randomPolygons()(width, height).map(_.envelope)
      extents foreach { extent =>
        val masked = rdd.mask(extent, options = opts).stitch.tile.band(0)
        val expected = tile.mask(worldExt, extent)
        masked.toArray() shouldEqual expected.toArray()
      }
    }
  }
}

