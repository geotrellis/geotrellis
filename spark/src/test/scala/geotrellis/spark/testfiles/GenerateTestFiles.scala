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

package geotrellis.spark.testfiles

import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.spark.utils._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4._
import org.apache.spark._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/** Use this command to create test files when there's a breaking change to the files (i.e. SpatialKeyWritable package move) */
object GenerateTestFiles {

  def main(args: Array[String]): Unit = {
    val cellType = TypeFloat
    val layoutLevel = ZoomedLayoutScheme().levelFor(10)
    val tileLayout = layoutLevel.tileLayout
    val (tileCols, tileRows) =  (tileLayout.tileCols, tileLayout.tileRows)
    val worldExtent = Extent(-180, -89.99999, 179.99999, 89.99999)
    val re = RasterExtent(worldExtent, tileCols, tileRows)
    val extent = Extent(141.7066666666667, -18.373333333333342, 142.56000000000003, -17.52000000000001)
    val gridBounds = re.gridBoundsFor(extent)

    val testFiles = List(
      ConstantTestFileValues(1) -> "all-ones",
      ConstantTestFileValues(2) -> "all-twos",
      ConstantTestFileValues(100) -> "all-hundreds",
      IncreasingTestFileValues(tileCols, tileRows) -> "increasing",
      DecreasingTestFileValues(tileCols, tileRows) -> "decreasing",
      EveryOtherUndefined(tileCols) -> "every-other-undefined",
      EveryOther0Point99Else1Point01(tileCols) -> "every-other-0.99-else-1.01",
      EveryOther1ElseMinus1(tileCols) -> "every-other-1-else-1"
    )

    val sc = new SparkContext("local", "create-test-files")
    val conf = sc.hadoopConfiguration
    val localFS = new Path(System.getProperty("java.io.tmpdir")).getFileSystem(conf)
    val catalogPath = new Path(localFS.getWorkingDirectory, "src/test/resources/")
    val catalog = HadoopCatalog(sc, catalogPath)

    for((tfv, name) <- testFiles) {
      println(s"Creating test RasterRDD $name")
      val tmsTiles =
        gridBounds.coords.map { case (col, row) =>
          val arr = tfv(tileCols, tileRows)
          (SpatialKey(col, row), ArrayTile(arr, tileCols, tileRows): Tile)
        }

      val rdd =
        asRasterRDD(RasterMetaData(cellType, extent, LatLng, tileLayout)) {
          sc.parallelize(tmsTiles)
        }

      catalog.save(LayerId(name, 10), rdd, clobber = true)
    }
  }

}
