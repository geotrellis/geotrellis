/*
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
 */

package geotrellis.raster

import geotrellis.vector._

import spire.syntax.cfor._

/**
 * MutableArrayTile is an ArrayTile whose cells can be written to
 * (mutated).
 */
trait MutableArrayTile extends ArrayTile {
  def mutable = this

  def update(i:Int, z:Int): Unit
  def updateDouble(i:Int, z:Double):Unit

  def set(col:Int, row:Int, value:Int) {
    update(row * cols + col, value)
  }
  def setDouble(col:Int, row:Int, value:Double) {
    updateDouble(row * cols + col, value)
  }
  def update(colOffset:Int, rowOffset:Int, update: Tile): Unit = {
    if (this.cellType.isFloatingPoint) {
      cfor(0)(_ < update.rows, _ + 1) { r =>
        cfor(0)(_ < update.cols, _ + 1) { c =>
          setDouble(c + colOffset, r + rowOffset, update.getDouble(c, r))
        }
      }
    } else {
      cfor(0)(_ < update.rows, _ + 1) { r =>
        cfor(0)(_ < update.cols, _ + 1) { c =>
          set(c + colOffset, r + rowOffset, update.get(c, r))
        }
      }
    }
  }
}
