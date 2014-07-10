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
import geotrellis.RasterExtent
import geotrellis.spark.TestEnvironment
import geotrellis.spark.testfiles.AllOnes

import org.scalatest._

class TmsTilingConvertSpec extends FunSpec with TestEnvironment with Matchers {
  val allOnes = AllOnes(inputHome, conf)
  val meta = allOnes.meta
  val te = meta.metadataForBaseZoom.tileExtent
  val layout = allOnes.tileLayout
  val rasterExtent = allOnes.rasterExtent
  val zoom = meta.maxZoomLevel
  val tileSize = meta.tileSize
  val res = TmsTiling.resolution(zoom, tileSize)

  describe("tile indexing conversion tests") {
    it("should yield the right gtTileId") {

      for {
        ty <- te.ymin to te.ymax
        tx <- te.xmin to te.xmax
        tileId = TmsTiling.tileId(tx, ty, zoom)
      } {
        val expGtX = (tx - te.xmin).toInt
        val expGtY = (te.ymax - ty).toInt
        TmsTilingConvert.toGtTileIdX(tx, te) should be(expGtX)
        TmsTilingConvert.toGtTileIdY(ty, te) should be(expGtY)
        TmsTilingConvert.toGtTileId(tileId, layout, te, zoom) should be(
          layout.getTileIndex(expGtX, expGtY))
      }
    }

    it("should yield the right tileId") {

      for {
        gtTy <- 0 until layout.tileRows
        gtTx <- 0 until layout.tileCols
        gtTileId = layout.getTileIndex(gtTx, gtTy)
      } {
        val expTx = te.xmin + gtTx
        val expTy = te.ymax - gtTy
        TmsTilingConvert.fromGtTileIdX(gtTx, te) should be(expTx)
        TmsTilingConvert.fromGtTileIdY(gtTy, te) should be(expTy)
        TmsTilingConvert.fromGtTileId(gtTileId, layout, te, zoom) should be(
          TmsTiling.tileId(expTx, expTy, zoom))
      }
    }
    it("should yield the correct tile extents") {

      for {
        ty <- te.ymin to te.ymax
        tx <- te.xmin to te.xmax
        tileId = TmsTiling.tileId(tx, ty, zoom)
      } {
        val expGtX = (tx - te.xmin).toInt
        val expGtY = (te.ymax - ty).toInt
        val rl = layout.getResolutionLayout(rasterExtent)

        val actRasterExtent = rl.getRasterExtent(expGtX, expGtY)
        val actExtent = rl.getExtent(expGtX, expGtY)

        val expExtent = TmsTiling.tileToExtent(tx, ty, zoom, tileSize)
        val expRasterExtent = RasterExtent(expExtent, res, res, tileSize, tileSize)

        actExtent should be(expExtent)
        actRasterExtent should be(expRasterExtent)
      }
    }
  }
}