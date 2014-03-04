/*******************************************************************************
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
 ******************************************************************************/

package geotrellis.raster

import geotrellis._
import java.nio.ByteBuffer

/**
 * RasterData based on Array[Short] (each cell as a Short).
 */
final case class ShortArrayRasterData(array: Array[Short], cols: Int, rows: Int)
  extends MutableRasterData with IntBasedArray {
  def getType = TypeShort
  def alloc(cols: Int, rows: Int) = ShortArrayRasterData.ofDim(cols, rows)
  def length = array.length
  def apply(i: Int) = s2i(array(i))
  def update(i: Int, z: Int) { array(i) = i2s(z) }
  def copy = ShortArrayRasterData(array.clone, cols, rows)

  def toArrayByte: Array[Byte] = {
    val pixels = new Array[Byte](array.length * getType.bytes)
    val bytebuff = ByteBuffer.wrap(pixels)
    bytebuff.asShortBuffer.put(array)
    pixels
  }

  def warp(current:RasterExtent,target:RasterExtent):RasterData = {
    val warped = Array.ofDim[Short](target.cols*target.rows).fill(shortNODATA)
    Warp[Short](current,target,array,warped)
    ShortArrayRasterData(
      warped,
      target.cols,
      target.rows
    )
  }
}

object ShortArrayRasterData {
  def ofDim(cols: Int, rows: Int) = 
    new ShortArrayRasterData(Array.ofDim[Short](cols * rows), cols, rows)
  def empty(cols: Int, rows: Int) = 
    new ShortArrayRasterData(Array.ofDim[Short](cols * rows).fill(shortNODATA), cols, rows)

  def fromArrayByte(bytes: Array[Byte], cols: Int, rows: Int) = {
    val byteBuffer = ByteBuffer.wrap(bytes, 0, bytes.length)
    val shortBuffer = byteBuffer.asShortBuffer()
    val shortArray = new Array[Short](bytes.length / TypeShort.bytes)
    shortBuffer.get(shortArray)

    ShortArrayRasterData(shortArray, cols, rows)
  }
}
