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

package geotrellis.raster.reproject

import geotrellis.raster._

object Implicits extends Implicits

trait Implicits {
  implicit class withSinglebandReprojectMethods(val self: Tile) extends SinglebandTileReprojectMethods
  implicit class withMultibandReprojectMethods(val self: MultibandTile) extends MultibandTileReprojectMethods

  implicit class withSinglebandRasterReprojectMethods(val self: Raster[Tile]) extends SinglebandRasterReprojectMethods
  implicit class withMultibandRasterReprojectMethods(val self: Raster[MultibandTile]) extends MultibandRasterReprojectMethods

  implicit class withSinglebandProjectedRasterReprojectMethods(val self: ProjectedRaster[Tile]) extends SinglebandProjectedRasterReprojectMethods
  implicit class withMultibandProjectedRasterReprojectMethods(val self: ProjectedRaster[MultibandTile]) extends MultibandProjectedRasterReprojectMethods

  implicit class withSinglebandTileFeatureReprojectMethods[D](val self: TileFeature[Tile, D]) extends SinglebandTileFeatureReprojectMethods[D]
  implicit class withMultibandTileFeatureReprojectMethods[D](val self: TileFeature[MultibandTile, D]) extends MultibandTileFeatureReprojectMethods[D]

  implicit class withSinglebandRasterTileFeatureReprojectMethods[D](val self: TileFeature[Raster[Tile], D]) extends SinglebandRasterTileFeatureReprojectMethods[D]
  implicit class withMultibandRasterTileFeatureReprojectMethods[D](val self: TileFeature[Raster[MultibandTile], D]) extends MultibandRasterTileFeatureReprojectMethods[D]
}
