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

package geotrellis.engine

import geotrellis.raster._
import geotrellis.vector.Extent
import geotrellis.vector.io.FileSystem
import geotrellis.raster.io.arg.ArgReader

import com.typesafe.config.Config
import java.io.File

import spire.syntax.cfor._
import scala.collection.mutable

object TileSetRasterLayerBuilder
extends RasterLayerBuilder {
  def apply(ds: Option[String], jsonPath: String, json: Config): RasterLayer = {
    val tileDir = 
      if(json.hasPath("path")) {
        val f = new File(json.getString("path"))
        if(f.isAbsolute) {
          f
        } else {
          new File(new File(jsonPath).getParent, f.getPath)
        }
      } else {
        // Default to a directory with the same name as the layer name.
        new File(new File(jsonPath).getParent, getName(json))
      }

    if(!tileDir.isDirectory) {
      throw new java.io.IOException(s"[ERROR] Raster in catalog points Tile Directory '${tileDir.getPath}'" +
                                     ", but this is not a valid directory.")
    } else {
      val tileDirPath = tileDir.getPath
      val layoutCols = json.getInt("layout_cols")
      val layoutRows = json.getInt("layout_rows")
      val pixelCols = json.getInt("pixel_cols")
      val pixelRows = json.getInt("pixel_rows")
      val cols = layoutCols * pixelCols
      val rows = layoutRows * pixelRows

      val (cw, ch) = getCellWidthAndHeight(json)

      val rasterExtent = RasterExtent(getExtent(json), cw, ch, cols, rows)
      val layout = TileLayout(layoutCols, layoutRows, pixelCols, pixelRows)

      val info = 
        RasterLayerInfo(
          LayerId(ds, getName(json)),
          getCellType(json),
          rasterExtent,
          getEpsg(json),
          getXskew(json),
          getYskew(json),
          layout,
          getCacheFlag(json)
        )

      new TileSetRasterLayer(info, tileDirPath, layout)
    }
  }
}

object TileSetRasterLayer {
  def tileName(id: LayerId, col: Int, row: Int) = 
    s"${id}_${col}_${row}"

  def tilePath(path: String, id: LayerId, col: Int, row: Int) =
    FileSystem.join(path, s"${id.name}_${col}_${row}.arg")
}

class TileSetRasterLayer(info: RasterLayerInfo,
                         val tileDirPath: String,
                         val tileLayout: TileLayout)
extends RasterLayer(info) {
  def getRaster(targetExtent: Option[RasterExtent]) = {
    targetExtent match {
      case Some(re) =>
        // If a specific raster extent is asked for,
        // load an ArrayRasterData for the extent.

        // Create destination raster data
        val tile = ArrayTile.empty(info.cellType, re.cols, re.rows)

        // Collect data from intersecting tiles
        val targetExtent = re.extent
        val tileExtents = TileExtents(info.rasterExtent.extent, tileLayout)
        val loader = getTileLoader()
        cfor(0)(_ < tileLayout.layoutRows, _ + 1) { trow =>
          cfor(0)(_ < tileLayout.layoutCols, _ + 1) { tcol =>
            val sourceExtent = tileExtents(tcol, trow)
            sourceExtent.intersection(targetExtent) match {
              case Some(ext) =>
                val cols = math.ceil((ext.xmax - ext.xmin) / re.cellwidth).toInt
                val rows = math.ceil((ext.ymax - ext.ymin) / re.cellheight).toInt
                val tileRe = RasterExtent(ext, re.cellwidth, re.cellheight, cols, rows)

                // Read section of the tile
                val rasterPart = loader.getTile(tcol, trow, Some(tileRe))

                // Copy over the values to the correct place in the raster data
                cfor(0)(_ < rows, _ + 1) { partRow =>
                  cfor(0)(_ < cols, _ + 1) { partCol =>
                    val dataCol = re.mapXToGrid(tileRe.gridColToMap(partCol))
                    val dataRow = re.mapYToGrid(tileRe.gridRowToMap(partRow))
                    if(!(dataCol < 0 || dataCol >= re.cols ||
                         dataRow < 0 || dataRow >= re.rows)) {
                      if(info.cellType.isFloatingPoint) {
                        tile.setDouble(dataCol, dataRow, rasterPart.getDouble(partCol, partRow))
                      } else {
                        tile.set(dataCol, dataRow, rasterPart.get(partCol, partRow))
                      }
                    }
                  }
                }

              case None => // pass
            }
          }
        }
        tile
      case None => 
        val loader = getTileLoader()
        val tiles = mutable.ListBuffer[Tile]()
        cfor(0)(_ < tileLayout.layoutRows, _ + 1) { row =>
          cfor(0)(_ < tileLayout.layoutCols, _ + 1) { col =>
            tiles += loader.getTile(col, row, None)
          }
        }
        CompositeTile(tiles.toSeq, tileLayout).toArrayTile
    }
  }

  override
  def getRaster(extent: Extent): Tile = 
    CroppedTile(getRaster(None), info.rasterExtent.gridBoundsFor(extent))

  def getTile(col: Int, row: Int, targetExtent: Option[RasterExtent]) = 
    getTileLoader().getTile(col, row, targetExtent)

  def getTileLoader() =
    if(isCached)
      new CacheTileLoader(info, tileLayout, getCache)
    else 
      new DiskTileLoader(info, tileLayout, tileDirPath)

  def cache(c: Cache[String]) = {
    for(col <- 0 until tileLayout.layoutCols) {
      for(row <- 0 until tileLayout.layoutRows) {
        val path = TileSetRasterLayer.tilePath(tileDirPath, info.id, col, row)
        c.insert(TileSetRasterLayer.tileName(info.id, col, row), FileSystem.slurp(path))
      }
    }
  }
}

abstract class TileLoader(tileSetInfo: RasterLayerInfo,
                          tileLayout: TileLayout) extends Serializable {
  val tileExtents = TileExtents(tileSetInfo.rasterExtent.extent, tileLayout)

  val rasterExtent = tileSetInfo.rasterExtent

  def getTile(col: Int, row: Int, targetExtent: Option[RasterExtent]): Tile = {
    val re = RasterExtent(tileExtents(col, row), tileLayout.tileCols, tileLayout.tileRows)
    if(col < 0 || row < 0 ||
       tileLayout.layoutCols <= col || tileLayout.layoutRows <= row) {
      val tre = 
        targetExtent match {
          case Some(x) => x
          case None => re
        }

      IntConstantTile(NODATA, tre.cols, tre.rows)
    } else {
      loadRaster(col, row, re, targetExtent)
    }
  }

  protected def loadRaster(col: Int, row: Int, re: RasterExtent, tre: Option[RasterExtent]): Tile
}

class DiskTileLoader(tileSetInfo: RasterLayerInfo,
                     tileLayout: TileLayout,
                     tileDirPath: String)
extends TileLoader(tileSetInfo, tileLayout) {
  def loadRaster(col: Int, row: Int, re: RasterExtent, targetExtent: Option[RasterExtent]): Tile = {
    val path = TileSetRasterLayer.tilePath(tileDirPath, tileSetInfo.id, col, row)
    targetExtent match {
      case Some(tre) => 
        ArgReader.read(path, tileSetInfo.cellType, re, tre)
      case None => 
        ArgReader.read(path, tileSetInfo.cellType, re.cols, re.rows)
    }

  }
}

class CacheTileLoader(info: RasterLayerInfo,
                      tileLayout: TileLayout,
                      c: Cache[String])
extends TileLoader(info, tileLayout) {
  def loadRaster(col: Int, row: Int, re: RasterExtent, targetExtent: Option[RasterExtent]): Tile = {
    c.lookup[Array[Byte]](TileSetRasterLayer.tileName(info.id, col, row)) match {
      case Some(bytes) =>
        targetExtent match {
          case Some(tre) => 
            ArgReader.resampleBytes(bytes, info.cellType, re, tre)
          case None => 
            ArrayTile.fromBytes(bytes, info.cellType, re.cols, re.rows)
        }
      case None =>
        sys.error("Cache problem: Tile thinks it's cached but it is in fact not cached.")
    }
  }
}
