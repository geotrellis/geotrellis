package geotrellis.raster.io.geotiff

import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.vector.Extent
import geotrellis.proj4.CRS

class MultibandGeoTiff(
  val tile: MultibandTile,
  val extent: Extent,
  val crs: CRS,
  val tags: Tags,
  options: GeoTiffOptions
) extends GeoTiff[MultibandTile] {
  def mapTile(f: MultibandTile => MultibandTile): MultibandGeoTiff =
    MultibandGeoTiff(f(tile), extent, crs, tags, options)

  def imageData: GeoTiffImageData =
    tile match {
      case gtt: GeoTiffMultibandTile => gtt
      case _ => GeoTiffMultibandTile(tile)
    }
}

object MultibandGeoTiff {
  /** Read a multi-band GeoTIFF file from a byte array.
    * GeoTIFF will be fully uncompressed and held in memory.
    */
  def apply(bytes: Array[Byte]): MultibandGeoTiff =
    GeoTiffReader.readMultiband(bytes)

  /** Read a multi-band GeoTIFF file from a byte array.
    * If decompress = true, the GeoTIFF will be fully uncompressed and held in memory.
    */
  def apply(bytes: Array[Byte], decompress: Boolean): MultibandGeoTiff =
    GeoTiffReader.readMultiband(bytes, decompress)

  /** Read a multi-band GeoTIFF file from the file at the given path.
    * GeoTIFF will be fully decompressed and held in memory.
    */
  def apply(path: String): MultibandGeoTiff =
    GeoTiffReader.readMultiband(path)

  /** Read a multi-band GeoTIFF file from the file at the given path.
    * If decompress = true, the GeoTIFF will be fully decompressed and held in memory.
    */
  def apply(path: String, decompress: Boolean): MultibandGeoTiff =
    GeoTiffReader.readMultiband(path, decompress)


  /** Read a multi-band GeoTIFF file from the file at a given path.
    * The tile data will remain tiled/striped and compressed in the TIFF format.
    */
  def compressed(path: String): MultibandGeoTiff =
    GeoTiffReader.readMultiband(path, false)


  /** Read a multi-band GeoTIFF file from a byte array.
    * The tile data will remain tiled/striped and compressed in the TIFF format.
    */
  def compressed(bytes: Array[Byte]): MultibandGeoTiff =
    GeoTiffReader.readMultiband(bytes, false)

  def apply(
    tile: MultibandTile,
    extent: Extent,
    crs: CRS
  ): MultibandGeoTiff =
    apply(tile, extent, crs, Tags.empty)

  def apply(
    tile: MultibandTile,
    extent: Extent,
    crs: CRS,
    tags: Tags
  ): MultibandGeoTiff =
    apply(tile, extent, crs, tags, GeoTiffOptions.DEFAULT)

  def apply(
    tile: MultibandTile,
    extent: Extent,
    crs: CRS,
    tags: Tags,
    options: GeoTiffOptions
  ): MultibandGeoTiff =
    new MultibandGeoTiff(tile, extent, crs, tags, options)

  def unapply(mbg: MultibandGeoTiff): Option[(MultibandTile, Extent, CRS, Tags)] =
    Some((mbg.tile, mbg.extent, mbg.crs, mbg.tags))

  implicit def multiBandGeoTiffToTile(mbg: MultibandGeoTiff): MultibandTile =
    mbg.tile
}
