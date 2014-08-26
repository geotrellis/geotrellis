package geotrellis.raster.io.arg

import geotrellis.raster._
import geotrellis.vector.Extent
import geotrellis.raster.io._
import spire.syntax.cfor._
import com.typesafe.config.ConfigFactory
import java.io.File
import java.nio.ByteBuffer
import geotrellis.raster.multiband.MultiBandTile

object MultibandArgReader {

  /** Reads multiband arg from the jsom metadata file. */
  final def read(path: String): MultiBandTile =
    readBands(path, None)

  /** Reads multiband arg from the jsom metadata file. */
  final def read(path: String, targetRasterExtent: RasterExtent): MultiBandTile =
    readBands(path, Some(targetRasterExtent))

  private final def readBands(path: String, targetRasterExtent: Option[RasterExtent]): MultiBandTile = {
    val json = ConfigFactory.parseString(Filesystem.readText(path))
    val layerType = json.getString("type").toLowerCase
    if (layerType != "arg") { sys.error(s"Cannot read raster layer type $layerType, must be arg") }

    val noOfBands: Int = json.getInt("bands")

    val argPath = {
      val paths: Array[String] = new Array[String](noOfBands)
      if (json.hasPath("path")) {
        val p = json.getString("path")
        cfor(0)(_ < noOfBands, _ + 1) { band =>
          paths(band) = p + "-band" + band + ".arg" 
        }
      } else {
        val layerName = json.getString("layer")
        // Default to a .arg file with the same name as the layer name.
        cfor(0)(_ < noOfBands, _ + 1) { band =>
          paths(band) = layerName + "-band" + band + ".arg" 
        }
      }
      paths
    }

    val cellType =
      json.getString("datatype") match {
        case "bool" => TypeBit
        case "int8" => TypeByte
        case "int16" => TypeShort
        case "int32" => TypeInt
        case "float32" => TypeFloat
        case "float64" => TypeDouble
        case s => sys.error("unsupported datatype '%s'" format s)
      }

    val cols = json.getInt("cols")
    val rows = json.getInt("rows")

    targetRasterExtent match {
      case Some(te) =>

        val xmin = json.getDouble("xmin")
        val ymin = json.getDouble("ymin")
        val xmax = json.getDouble("xmax")
        val ymax = json.getDouble("ymax")
        val extent = Extent(xmin, ymin, xmax, ymax)

        MultiBandTile(argPath.map(f => ArgReader.read(f, cellType, RasterExtent(extent, cols, rows), te)))
      case None =>
        MultiBandTile(argPath.map(f => ArgReader.read(f, cellType, cols, rows)))
    }
  }

}