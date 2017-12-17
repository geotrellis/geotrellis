package geotrellis.spark.io.cog

import geotrellis.raster.{CellGrid, GridBounds}
import geotrellis.raster.io.geotiff.GeoTiff

import java.net.URI

trait TiffMethods[V <: CellGrid] {
  def readTiff(uri: URI, index: Int): GeoTiff[V]
  def readTiff(bytes: Array[Byte], index: Int): GeoTiff[V] = { null }
  def tileTiff[K](tiff: GeoTiff[V], gridBounds: GridBounds): V
  def getSegmentGridBounds(uri: URI, index: Int): (Int, Int) => GridBounds
  def getSegmentGridBounds(bytes: Array[Byte], index: Int): (Int, Int) => GridBounds = { null }
}
