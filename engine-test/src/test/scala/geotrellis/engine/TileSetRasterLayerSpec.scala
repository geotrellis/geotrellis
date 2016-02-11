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

package geotrellis.process

import geotrellis.engine._
import geotrellis.vector.Extent
import geotrellis.raster._

import org.scalatest._

import scala.math.abs

class TileSetRasterLayerSpec extends FunSpec with Matchers with TestEngine  {
  describe("A TileSetRasterLayer") {
    it("should get a cropped version correctly") {
      val re = RasterSource("albers_DevelopedLand").rasterExtent.get
      val Extent(xmin, ymin, xmax, ymax) = re.extent
      val newRe = 
        RasterExtent(Extent(xmin,ymin,(xmin+xmax)/2.0,(ymin+ymax)/2.0),
                     re.cellwidth,
                     re.cellheight,
                     re.cols/2,
                     re.rows/2)

      val rs = RasterSource("albers_DevelopedLand", newRe)

      rs.rasterExtent.get should be (newRe)
    }
  }

}
