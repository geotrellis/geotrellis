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

package geotrellis.raster.io.geotiff.reader

import monocle.syntax._

import geotrellis.raster.io.geotiff.reader.ImageDirectoryLenses._
import geotrellis.raster.io.geotiff.reader.Orientations._

import spire.syntax.cfor._

object OrientationConverter {

  def apply(directory: ImageDirectory) = new OrientationConverter(
    directory.bitsPerPixel,
    directory |-> orientationLens get,
    directory |-> imageLengthLens get,
    directory |-> imageWidthLens get
  )

}

class OrientationConverter(
  bitsPerPixel: Int,
  orientation: Int,
  rows: Int,
  cols: Int) {

  def setCorrectOrientation(image: Array[Byte]): Array[Byte] = orientation match {
    case TopLeft => image
    case TopRight => convertTopRight(image)
    case BottomLeft => convertBottomLeft(image)
    case BottomRight => convertBottomRight(image)
    case LeftTop => convertLeftTop(image)
    case RightTop => convertRightTop(image)
    case RightBottom => convertRightBottom(image)
    case LeftBottom => convertLeftBottom(image)
    case _ =>
      throw new MalformedGeoTiffException(s"Orientation $orientation is not correct.")
  }

  private def convertTopRight(image: Array[Byte]) = {
    if (bitsPerPixel != 1) {
      val bytes = bitsPerPixel / 8

      cfor(0)(_ < rows, _ + 1) { i =>
        cfor(0)(_ < cols / 2, _ + 1) { j =>
          cfor(0)(_ < bytes, _ + 1) { k =>
            val firstIndex = cols * i * bytes + (j * bytes) + k
            val secondIndex = cols * (i + 1) * bytes - (j * bytes) + k - bytes

            val t = image(firstIndex)
            image(firstIndex) = image(secondIndex)
            image(secondIndex) = t
          }
        }
      }
    } else {
      cfor(0)(_ < rows, _ + 1) { i =>
        cfor(0)(_ < cols / 2, _ + 1) { j =>
          val firstIndex = (cols * i + j)
          val firstByteIndex = firstIndex / 8

          val secondIndex = (cols * (i + 1) - j - 1)
          val secondByteIndex = secondIndex / 8

          val firstBitIndex = (Int.MaxValue - firstIndex) % 8 //TODO
          val secondBitIndex = (Int.MaxValue - secondIndex) % 8

          val first = (image(firstByteIndex) & (1 << firstBitIndex)) != 0
          val second = (image(secondByteIndex) & (1 << secondBitIndex)) != 0

          if (first != second) {
            image(firstByteIndex) = (image(firstByteIndex) ^ (1 << firstBitIndex)).toByte
            image(secondByteIndex) = (image(secondByteIndex) ^ (1 << secondBitIndex)).toByte
          }
        }
      }
    }

    image
  }

  private def convertBottomLeft(image: Array[Byte]) = ???

  private def convertBottomRight(image: Array[Byte]) = ???

  private def convertLeftTop(image: Array[Byte]) = ???

  private def convertRightTop(image: Array[Byte]) = ???

  private def convertRightBottom(image: Array[Byte]) = ???

  private def convertLeftBottom(image: Array[Byte]) = ???

}
