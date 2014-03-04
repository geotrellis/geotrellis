/*******************************************************************************
 * Copyright (c) 2014 Azavea.
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
 ******************************************************************************/

package geotrellis.raster.op.zonal.summary

import geotrellis._
import geotrellis.source._
import geotrellis.feature._
import geotrellis.feature.rasterize._
import geotrellis.statistics._

object Histogram extends TileSummary[Histogram,Histogram,ValueSource[Histogram]] {
  def handlePartialTile[D](pt:PartialTileIntersection[D]):Histogram = {
    val PartialTileIntersection(r,polygons) = pt
    val histogram = FastMapHistogram()
    for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
      Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
        new Callback[Geometry,D] {
          def apply (col:Int, row:Int, g:Geometry[D]) {
            val z = r.get(col,row)
            if (isData(z)) histogram.countItem(z, 1)
          }
        }
      )
    }

    histogram
  }

  def handleFullTile(ft:FullTileIntersection):Histogram = {
    val histogram = FastMapHistogram()
    ft.tile.foreach((z:Int) => if (isData(z)) histogram.countItem(z, 1))
    histogram
  }

  def converge(ds:DataSource[Histogram,_]) =
    ds.map(x=>x).converge // Map to kick in the CanBuildFrom for HistogramDS
}
