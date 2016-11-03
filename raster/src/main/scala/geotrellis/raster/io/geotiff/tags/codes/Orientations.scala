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

package geotrellis.raster.io.geotiff.tags.codes

/**
  * The Orientations are named as such as the first position is where
  * the rows start and the second where the columns start.
  *
  * For example TopLeft means that the the 0th row is the top of the image
  * and the 0th column is the left of the image.
  */
object Orientations {

  val TopLeft = 1
  val TopRight = 2
  val BottomRight = 3
  val BottomLeft = 4
  val LeftTop = 5
  val RightTop = 6
  val RightBottom = 7
  val LeftBottom = 8

}
