package geotrellis.statistics.op.stat

import geotrellis._
import geotrellis.geometry._
import geotrellis.geometry.rasterizer.Rasterizer
import geotrellis.data._
import geotrellis.statistics._

import scala.math.{max,min}

/**
 * Given a raster and an array of polygons, return a histogram summary of the cells
 * within each polygon.
 */
case class GetPolygonalZonalHistograms(ps:Array[Op[Polygon]], r:Op[Raster],
                                    size:Int) extends Op[Array[Histogram]] {
  def _run(context:Context) = runAsync(r :: ps.toList)

  val nextSteps:Steps = {
    case raster :: polygons => {
      step2(raster.asInstanceOf[Raster], polygons.asInstanceOf[List[Polygon]])
    }
  }

  def step2(raster:Raster, polygons:List[Polygon]) = {
    // build our map to hold results
    val histmap = Array.ofDim[Histogram](size)

    // find all the unique values
    polygons.foreach {
      p => if (histmap(p.value) == null) {
        histmap(p.value) = ArrayHistogram(this.size)
      }
    }

    // dereference some useful variables
    val geo   = raster.rasterExtent
    val rdata = raster.data.asArray.getOrElse(sys.error("need array"))

    val p0    = polygons(0)
    val rows  = geo.rows
    val cols  = geo.cols

    // calculate the bounding box
    var xmin = p0.xmin
    var ymin = p0.ymin
    var xmax = p0.xmax
    var ymax = p0.ymax
    polygons.tail.foreach {
      p => {
        xmin = min(xmin, p.xmin)
        ymin = min(ymin, p.ymin)
        xmax = max(xmax, p.xmax)
        ymax = max(ymax, p.ymax)
      }
    }

    // save the bounding box as grid coordinates
    val (col1, row1) = geo.mapToGrid(xmin, ymin)
    val (col2, row2) = geo.mapToGrid(xmax, ymax)

    // burn our polygons onto a raster
    val zones = Raster.empty(geo)
    val zdata = zones.data.asArray.getOrElse(sys.error("need array"))
    Rasterizer.rasterize(zones, polygons.toArray)

    // iterate over the cells in our bounding box; determine its zone, then
    // looking in the raster for a value to add to the zonal histogram.
    var row = row1
    while (row < row2) {
      var col = col1
      while (col < col2) {
        val i     = row * cols + col
        val value = rdata(i)
        if (value != NODATA) {
          val zone  = zdata(i)
          if (zone != NODATA) {
            val histogram = histmap(zone)
            histogram.countItem(value)
          }
        }
        col += 1
      }
      row += 1
    }

    // return an immutable mapping
    Result(histmap)
  }

}
