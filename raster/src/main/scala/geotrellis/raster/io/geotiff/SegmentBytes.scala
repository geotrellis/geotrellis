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

package geotrellis.raster.io.geotiff

import spire.syntax.cfor._

/**
 * The base trait of SegmentBytes. It can be implemented either as
 * an Array[Array[Byte]] or as a ByteBuffer that is lazily read in.
 */
trait SegmentBytes extends Traversable[Array[Byte]] {
  def getSegment(i: Int): Array[Byte]

  def foreach[U](f: Array[Byte] => U): Unit =
    cfor(0)(_ < size, _ + 1) { i =>
      f(getSegment(i))
    }
}
