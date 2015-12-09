package geotrellis.engine

import geotrellis.engine._

package object render {
  implicit class RenderRasterSourceMethodExtensions(val rasterSource: RasterSource) 
      extends RenderRasterSourcePngMethods with RenderRasterSourceJpgMethods with RenderRasterSourceSharedMethods
}
