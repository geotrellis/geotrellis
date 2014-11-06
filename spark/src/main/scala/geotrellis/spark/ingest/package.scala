package geotrellis.spark

import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject._

import geotrellis.proj4.CRS

import org.apache.spark.rdd._

import monocle._
import monocle.syntax._

import spire.syntax.cfor._

import scala.reflect.ClassTag

package object ingest {
  type IngestKey[T] = SimpleLens[T, ProjectedExtent]

  implicit class IngestKeyWrapper[T: IngestKey](key: T) {
    val _projectedExtent = implicitly[IngestKey[T]]

    def projectedExtent: ProjectedExtent =
      key |-> _projectedExtent get

    def updateProjectedExtent(pe: ProjectedExtent): T =
      key |-> _projectedExtent set(pe)
  }

  // TODO: Move this to geotrellis.vector
  case class ProjectedExtent(extent: Extent, crs: CRS)
  object ProjectedExtent {
    implicit def ingestKey: IngestKey[ProjectedExtent] = SimpleLens(x => x, (_, x) => x)
  }

  implicit def projectedExtentToSpatialKeyTiler: Tiler[ProjectedExtent, SpatialKey] =
    new Tiler[ProjectedExtent, SpatialKey] {
      def getExtent(inKey: ProjectedExtent): Extent = inKey.extent
      def createKey(inKey: ProjectedExtent, spatialComponent: SpatialKey): SpatialKey = spatialComponent
    }

  implicit class ReprojectWrapper[T: IngestKey](rdd: RDD[(T, Tile)]) {
    val _projectedExtent = implicitly[IngestKey[T]]
    def reproject(destCRS: CRS): RDD[(T, Tile)] =
      rdd.map { case (key, tile) =>
        val ProjectedExtent(extent, crs) = key |-> _projectedExtent get
        val (newTile, newExtent) = tile.reproject(extent, crs, destCRS)
        (key |-> _projectedExtent set(ProjectedExtent(newExtent, destCRS))) -> newTile
      }
  }

  /** Tile methods used by the mosaicing function to merge tiles. */
  implicit class TileMerger(val tile: MutableArrayTile) {
    def merge(other: Tile): MutableArrayTile = {
      Seq(tile, other).assertEqualDimensions
      if(tile.cellType.isFloatingPoint) {
        cfor(0)(_ < tile.rows, _ + 1) { row =>
          cfor(0)(_ < tile.cols, _ + 1) { col =>
            if(isNoData(tile.getDouble(col, row))) {
              tile.setDouble(col, row, other.getDouble(col, row))
            }
          }
        }
      } else {
        cfor(0)(_ < tile.rows, _ + 1) { row =>
          cfor(0)(_ < tile.cols, _ + 1) { col =>
            if(isNoData(tile.get(col, row))) {
              tile.setDouble(col, row, other.get(col, row))
            }
          }
        }
      }

      tile
    }

    def merge(extent: Extent, otherExtent: Extent, other: Tile): MutableArrayTile =
      otherExtent & extent match {
        case PolygonResult(sharedExtent) =>
          val re = RasterExtent(extent, tile.cols, tile.rows)
          val GridBounds(colMin, rowMin, colMax, rowMax) = re.gridBoundsFor(sharedExtent)
          val otherRe = RasterExtent(otherExtent, other.cols, other.rows)

          def thisToOther(col: Int, row: Int): (Int, Int) = {
            val (x, y) = re.gridToMap(col, row)
            otherRe.mapToGrid(x, y)
          }

          if(tile.cellType.isFloatingPoint) {
            cfor(rowMin)(_ <= rowMax, _ + 1) { row =>
              cfor(colMin)(_ <= colMax, _ + 1) { col =>
                if(isNoData(tile.getDouble(col, row))) {
                  val (otherCol, otherRow) = thisToOther(col, row)
                  if(otherCol >= 0 && otherCol < other.cols &&
                    otherRow >= 0 && otherRow < other.rows)
                    tile.setDouble(col, row, other.getDouble(otherCol, otherRow))
                }
              }
            }
          } else {
            cfor(rowMin)(_ <= rowMax, _ + 1) { row =>
              cfor(colMin)(_ <= colMax, _ + 1) { col =>
                if(isNoData(tile.get(col, row))) {
                  val (otherCol, otherRow) = thisToOther(col, row)
                  if(otherCol >= 0 && otherCol < other.cols &&
                    otherRow >= 0 && otherRow < other.rows)
                    tile.set(col, row, other.get(otherCol, otherRow))
                }
              }
            }

          }

          tile
        case _ =>
          tile
      }
  }
}
