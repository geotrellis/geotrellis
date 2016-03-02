package geotrellis.raster.render

import geotrellis.raster._
import geotrellis.raster.render.jpg._
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.summary._
import geotrellis.util.MethodExtensions


trait JpgRenderMethods extends MethodExtensions[Tile] {
  /** Generate a JPG from a raster of RGBA integer values.
    *
    * Use this operation when you have created a raster whose values are already
    * RGBA color values that you wish to render into a JPG. If you have a raster
    * with data that you wish to render, you should use RenderJpg instead.
    *
    * An RGBA value is a 32 bit integer with 8 bits used for each component:
    * the first 8 bits are the red value (between 0 and 255), then green, blue,
    * and alpha (with 0 being transparent and 255 being opaque).
    *
    */
  def renderJpg(): Jpg =
    new JpgEncoder().writeByteArray(self)

  /**
    * Generate a JPG image from a raster.
    *
    * Use this operation when you have a raster of data that you want to visualize
    * with an image.
    *
    * To render a data raster into an image, the operation needs to know which
    * values should be painted with which colors.  To that end, you'll need to
    * generate a ColorBreaks object which represents the value ranges and the
    * assigned color.  One way to create these color breaks is to use the
    * [[geotrellis.raster.stats.op.stat.GetClassBreaks]] operation to generate
    * quantile class breaks.
    */
  def renderJpg(colorClassifier: ColorClassifier[_]): Jpg =
    renderJpg(colorClassifier, None)

  def renderJpg(colors: Array[RGBA]): Jpg = {
    val histogram = self.histogram
    val colorClassifier = StrictColorClassifier.fromQuantileBreaks(histogram, colors)
    renderJpg(colorClassifier, Some(histogram))
  }

  /**
    * Generate a JPG image from a raster.
    *
    * Use this operation when you have a raster of data that you want to visualize
    * with an image.
    *
    * To render a data raster into an image, the operation needs to know which
    * values should be painted with which colors.  To that end, you'll need to
    * generate a ColorBreaks object which represents the value ranges and the
    * assigned color.  One way to create these color breaks is to use the
    * [[geotrellis.raster.stats.op.stat.GetClassBreaks]] operation to generate
    * quantile class breaks.
    */
  def renderJpg(colorClassifier: ColorClassifier[_], histogram: Histogram[Int]): Jpg =
    renderJpg(colorClassifier, Some(histogram))

  private
  def renderJpg(colorClassifier: ColorClassifier[_], histogram: Option[Histogram[Int]]): Jpg = {
    val cmap = colorClassifier.toColorMap(histogram)
    val r2 = self.cellType match {
      case ct: ConstantNoData =>
        cmap.render(self).convert(ByteConstantNoDataCellType)
      case ct: UByteCells with UserDefinedNoData[Byte] =>
        cmap.render(self).convert(UByteUserDefinedNoDataCellType(ct.noDataValue))
      case ct: UShortCells with UserDefinedNoData[Short] =>
        cmap.render(self).convert(UShortUserDefinedNoDataCellType(ct.noDataValue))
      case _ =>
        cmap.render(self).convert(ByteCellType)
    }
    new JpgEncoder().writeByteArray(r2)
  }
}
