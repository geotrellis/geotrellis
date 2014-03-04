/*******************************************************************************
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
 ******************************************************************************/

package geotrellis
import geotrellis.spark.formats.ArgWritable
import geotrellis.spark.formats.TileIdWritable
import geotrellis.spark.metadata.Context
import geotrellis.spark.rdd.RasterRDD
import geotrellis.spark.rdd.SaveRasterFunctions

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD

package object spark {

  type TileIdRaster = (Long, Raster)
  type TileIdCoordRaster = (Long, Long, Long, Raster)
  type TileIdArgWritable = (TileIdWritable, ArgWritable)

  implicit class SavableRasterWritable(val raster: RDD[TileIdArgWritable]) {
    def save(path: Path) = SaveRasterFunctions.save(raster, path)
  }

  implicit class MakeRasterRDD(val prev: RDD[TileIdRaster]) {
    def withContext(ctx: Context) = new RasterRDD(prev, ctx)
  }
  implicit class SavableRasterRDD(val rdd: RasterRDD) {
    def save(path: Path) = SaveRasterFunctions.save(rdd, path)
  }
}