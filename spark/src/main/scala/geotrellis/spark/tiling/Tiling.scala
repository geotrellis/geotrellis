/*
 * Copyright (c) 2014 DigitalGlobe.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.tiling

import geotrellis.feature.Extent

case class TileCoord(tx: Long, ty: Long)

/**
 * Span of consecutive tile IDs, inclusive
 * @param min minimum TMS Tile ID in the span
 * @param max maximum TMS Tile ID in the span
 */
case class TileSpan(min: Long, max: Long)

/* Represents the dimensions of the tiles of a RasterRDD
 * based on a zoom level and world grid.
 */
case class TileExtent(xmin: Long, ymin: Long, xmax: Long, ymax: Long) {
  def width = xmax - xmin + 1
  def height = ymax - ymin + 1

  /**
   * Return a range from min tileId to max tileID for every row in the extent
   */
  def getRowRanges(zoom: Int): Seq[TileSpan] =
    for (y <- ymin to ymax)
    yield TileSpan(TmsTiling.tileId(xmin, y, zoom), TmsTiling.tileId(xmax, y, zoom))

  def contains(zoom: Int)(tileId: Long) = {
    val (x, y) = TmsTiling.tileXY(tileId, zoom)
    (x <= xmax && x >= xmin) && (y <= ymax && y >= ymin)
  }
} 

/* Represents the width and hieght of the raster pre-ingest 
 * (which might not match up with the TileExtent based on tile division).
 * @note    width/height is non-inclusive 
 */
case class PixelExtent(xmin: Long, ymin: Long, xmax: Long, ymax: Long) {
  def width = xmax - xmin
  def height = ymax - ymin
}

case class Pixel(px: Long, py: Long)

object Bounds {
  final val World = Extent(-180, -90, 179.99999, 89.99999)
}
