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

package geotrellis.spark.op.local

import geotrellis.spark._
import geotrellis.raster.op.local.Divide
import geotrellis.spark.rdd.RasterRDD

trait DivideRasterRDDMethods extends RasterRDDMethods {
  /** Divide each value of the raster by a constant value.*/
  def localDivide(i: Int): RasterRDD =
    rasterRDD.mapTiles { case TmsTile(t, r) => TmsTile(t, Divide(r, i)) }
  /** Divide each value of the raster by a constant value.*/
  def /(i: Int): RasterRDD = localDivide(i)
  /** Divide a constant value by each cell value.*/
  def localDivideValue(i: Int): RasterRDD =
    rasterRDD.mapTiles { case TmsTile(t, r) => TmsTile(t, Divide(i, r)) }
  /** Divide a constant value by each cell value.*/
  def /:(i: Int): RasterRDD = localDivideValue(i)
  /** Divide each value of a raster by a double constant value.*/
  def localDivide(d: Double): RasterRDD =
    rasterRDD.mapTiles { case TmsTile(t, r) => TmsTile(t, Divide(r, d)) }
  /** Divide each value of a raster by a double constant value.*/
  def /(d: Double): RasterRDD = localDivide(d)
  /** Divide a double constant value by each cell value.*/
  def localDivideValue(d: Double): RasterRDD =
    rasterRDD.mapTiles { case TmsTile(t, r) => TmsTile(t, Divide(d, r)) }
  /** Divide a double constant value by each cell value.*/
  def /:(d: Double): RasterRDD = localDivideValue(d)
  /** Divide the values of each cell in each raster. */
  def localDivide(other: RasterRDD): RasterRDD =
    rasterRDD.combineTiles(other) {
      case (TmsTile(t1, r1), TmsTile(t2, r2)) => TmsTile(t1, Divide(r1, r2))
    }
  /** Divide the values of each cell in each raster. */
  def /(other: RasterRDD): RasterRDD = localDivide(other)

  def localDivide(others: Traversable[RasterRDD]): RasterRDD =
    rasterRDD.combineTiles(others.toSeq) {
      case tmsTiles: Seq[TmsTile] =>
        TmsTile(tmsTiles.head.id, Divide(tmsTiles.map(_.tile)))
    }

  def /(others: Traversable[RasterRDD]): RasterRDD = localDivide(others)
}
