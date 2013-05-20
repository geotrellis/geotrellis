package geotrellis.data

import scala.math.{Numeric, min, max, abs, round, floor, ceil}
import java.io.{File, FileInputStream, FileOutputStream}

import geotrellis._
import geotrellis.process._
import geotrellis.raster.IntConstant

trait ReadState {
  val rasterExtent:RasterExtent
  val target:RasterExtent

  // must override
  def getType: RasterType

  // must override
  def createRasterData(cols:Int, rows:Int):MutableRasterData = RasterData.emptyByType(getType, cols, rows)

  // must override
  protected[this] def initSource(position:Int, size:Int):Unit

  // must override
  protected[this] def assignFromSource(sourceIndex:Int, dest:MutableRasterData, destIndex:Int):Unit

  // don't usually override
  protected[this] def createRaster(data:MutableRasterData) = Raster(data, target)

  // maybe override
  def destroy() {}

  // maybe need to override; currently a noop
  protected[this] def translate(data:MutableRasterData): Unit = ()

  // don't override
  def loadRaster(): Raster = {
    val re = rasterExtent

    // keep track of cell size in our source raster
    val src_cellwidth =  re.cellwidth
    val src_cellheight = re.cellheight
    val src_cols = re.cols
    val src_rows = re.rows
    val src_xmin = re.extent.xmin
    val src_ymin = re.extent.ymin
    val src_xmax = re.extent.xmax
    val src_ymax = re.extent.ymax

    // the dimensions to resample to
    val dst_cols = target.cols
    val dst_rows = target.rows

    // calculate the dst cell size
    val dst_cellwidth  = (target.extent.xmax - target.extent.xmin) / dst_cols
    val dst_cellheight = (target.extent.ymax - target.extent.ymin) / dst_rows

    // save "normalized map coordinates" for destination cell (0, 0)
    val xbase = target.extent.xmin - src_xmin + (dst_cellwidth / 2)
    val ybase = target.extent.ymax - src_ymin - (dst_cellheight / 2)

    // track height/width in map units
    val src_map_width  = src_xmax - src_xmin
    val src_map_height = src_ymax - src_ymin

    // initialize the whole raster
    // TODO: only initialize the part we will read from
    val src_size = src_rows * src_cols
    initSource(0, src_size)
    
    // this is the resampled destination array
    val dst_size = dst_cols * dst_rows
    val resampled = createRasterData(dst_cols, dst_rows)

    // these are the min and max columns we will access on this row
    val min_col = (xbase / src_cellwidth).asInstanceOf[Int]
    val max_col = ((xbase + dst_cols * dst_cellwidth) / src_cellwidth).asInstanceOf[Int]

    // start at the Y-center of the first dst grid cell
    var y = ybase

    // loop over rows
    var dst_row = 0
    while (dst_row < dst_rows) {

      // calculate the Y grid coordinate to read from
      val src_row = (src_rows - (y / src_cellheight).asInstanceOf[Int] - 1)
      //assert(src_row < src_rows)

      // pre-calculate some spans we'll use a bunch
      val src_span = src_row * src_cols
      val dst_span = dst_row * dst_cols

      // xyz
      if (src_span + min_col < src_size && src_span + max_col >= 0) {

        // start at the X-center of the first dst grid cell
        var x = xbase
  
        // loop over cols
        var dst_col = 0
        while (dst_col < dst_cols) {
  
          // calculate the X grid coordinate to read from
          val src_col = (x / src_cellwidth).asInstanceOf[Int]
          //assert(src_col < src_cols)
  
          // compute src and dst indices and ASSIGN!
          val src_i = src_span + src_col

          if (src_col >= 0 && src_col < src_cols && src_i < src_size && src_i >= 0) {

            val dst_i = dst_span + dst_col
            assignFromSource(src_i, resampled, dst_i)
          }
  
          // increase our X map coordinate
          x += dst_cellwidth
          dst_col += 1
        }
      }

      // decrease our Y map coordinate
      y -= dst_cellheight
      dst_row += 1
    }

    // build a raster object from our array and return
    translate(resampled)
    createRaster(resampled)
  }
}

trait IntReadState extends ReadState {
  // must override
  def getNoDataValue:Int

  protected[this] override def translate(data:MutableRasterData) {
    var i = 0
    val len = data.length
    val nd = getNoDataValue
    while (i < len) {
      if (data(i) == nd) data(i) = NODATA
      i += 1
    }
  }
}
