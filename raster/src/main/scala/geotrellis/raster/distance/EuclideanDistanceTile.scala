/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.raster.distance

import com.vividsolutions.jts.geom.Coordinate

import geotrellis.raster.{RasterExtent, DoubleArrayTile, Tile}
import geotrellis.raster.rasterize.polygon.PolygonRasterizer
import geotrellis.vector.{Point, Polygon}
import geotrellis.vector.voronoi._
import scala.math.sqrt

object EuclideanDistanceTile {

  private def fillFn(rasterExtent: RasterExtent, tile: DoubleArrayTile, base: Point)(col: Int, row: Int): Unit = {
    val (x,y) = rasterExtent.gridToMap(col, row)
    val currentValue = tile.getDouble(col, row)
    val newValue = sqrt((x - base.x) * (x - base.x) + (y - base.y) * (y - base.y))

    if (currentValue.isNaN || currentValue > newValue)
      tile.setDouble(col, row, newValue)
  }

  private def rasterizeDistanceCell(rasterExtent: RasterExtent, tile: DoubleArrayTile)(arg: (Polygon, Coordinate)) = {
    val (poly, coord) = arg

    val buffered = poly.buffer(math.max(rasterExtent.cellwidth, rasterExtent.cellheight))
    PolygonRasterizer.foreachCellByPolygon(buffered, rasterExtent)(fillFn(rasterExtent, tile, Point.jtsCoord2Point(coord)))
  }

  def apply(pts: Array[Point], rasterExtent: RasterExtent): Tile = {
    val vor = VoronoiDiagram(pts.map{ pt => new Coordinate(pt.x, pt.y) }, rasterExtent.extent)
    val tile = DoubleArrayTile.empty(rasterExtent.cols, rasterExtent.rows)

    vor.voronoiCellsWithPoints.foreach(rasterizeDistanceCell(rasterExtent, tile))
    tile
  }

  def apply(pts: Array[Coordinate], rasterExtent: RasterExtent): Tile = {
    val vor = VoronoiDiagram(pts, rasterExtent.extent)
    val tile = DoubleArrayTile.empty(rasterExtent.cols, rasterExtent.rows)

    vor.voronoiCellsWithPoints.foreach(rasterizeDistanceCell(rasterExtent, tile))
    tile
  }

}
