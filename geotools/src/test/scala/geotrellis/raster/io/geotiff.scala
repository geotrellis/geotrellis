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

package geotrellis.geotools

import geotrellis.engine._
import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.raster.io.geotiff.GeoTiffWriter
import geotrellis.vector.Extent
import geotrellis.raster.stats.FastMapHistogram
import geotrellis.proj4._

import geotrellis.testkit._

import org.scalatest._

import org.geotools.gce.geotiff.GeoTiffFormat
import org.geotools.factory.Hints
import org.geotools.referencing.CRS
import org.geotools.coverage.grid.GridCoordinates2D

import java.io.{File,FileWriter}
import javax.imageio.ImageIO

import scala.math.{abs, round}

import org.geotools.coverage.grid.io.imageio.geotiff.GeoTiffIIOMetadataDecoder
import org.geotools.coverage.grid.io.imageio.IIOMetadataDumper
import java.awt.image.BufferedImage

import java.awt.image.DataBuffer
import java.awt.Transparency

class GeoTiffSpec extends FunSpec with TestEngine with Matchers {
  describe("A GeoTiffReader") {
    it ("should fail on non-existent files") {
      val path = "/does/not/exist.tif"
      an [Exception] should be thrownBy { GeoTiffReader.read(path) }
    }

    it ("should load correct extent & gridToMap should work") {
      val path = "raster-test/data/econic.tif"
      val (tile, extent) = GeoTiffReader.read(path)
      val rasterExtent = RasterExtent(extent, tile.cols, tile.rows)
      val (xmap, ymap) = rasterExtent.gridToMap(0,0)
      xmap should be (-15381.615 +- 0.001)
      ymap should be (15418.729 +- 0.001)
    }

    it("should correctly translate NODATA values for an int raster which has no NODATA value associated") {
      val path = "geotools/data/cea.tif"
      val (raster, rasterExtent) = GeoTiffReader.read(path)
      val (cols, rows) = (raster.cols, raster.rows)

      // Find NODATA value
      val reader = GeoTiffReader.getReader(path)

      val nodata = reader.getMetadata().getNoData()
      val geoRaster = reader.read(null).getRenderedImage.getData
      val data = Array.fill(cols * rows)(nodata)
      geoRaster.getPixels(0, 0, cols, rows, data)

      val rdata = raster.toArray

      for(col <- 0 until cols) {
        for(row <- 0 until rows) {
          if(isNoData(rdata(row*cols + col))) {
            val v = data(row*cols + col)
            if(isNoData(nodata))
              isNoData(v) should be (true)
            else
              v should be (nodata)
          }
        }
      }
    }

    it ("should write and read back in the same") {
      val (r, extent) = GeoTiffReader.read("raster-test/data/econic.tif")
      GeoTiffWriter.write("/tmp/written.tif", r, extent, LatLng)
      val (r2, extent2) = GeoTiffReader.read("/tmp/written.tif")
      extent should be (extent)
      assertEqual(r, r2)
    }

    it ("should translate NODATA correctly") {
      // This test was set up by taking an ARG, slope.arg, and converting it to TIF using GDAL.
      // Then gdalwarp was used to change the NoData value for slope.tif to -9999.
      // If the NoData values are translated correctly, then all NoData values from the read in GTiff
      // should correspond to NoData values of the directly read arg.
      val originalArg = RasterSource.fromPath("raster-test/data/data/slope.arg").get
      val (translatedTif, _) = GeoTiffReader.read("raster-test/data/slope.tif")

      translatedTif.rows should be (originalArg.rows)
      translatedTif.cols should be (originalArg.cols)
      for(col <- 0 until originalArg.cols) {
        for(row <- 0 until originalArg.rows) {
          if(isNoData(originalArg.getDouble(col,row)))
             isNoData(translatedTif.getDouble(col,row)) should be (true)
          else
            translatedTif.getDouble(col,row) should be (originalArg.getDouble(col,row))
        }
      }
    }
  }
}
