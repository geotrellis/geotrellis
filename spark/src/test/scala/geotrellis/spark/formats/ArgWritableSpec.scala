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

package geotrellis.spark.formats

import geotrellis._
import geotrellis.raster._

import org.scalatest._

class ArgWritableSpec extends FunSpec with Matchers {
  describe("conversion from/to ArgWritable") {

    val cols = 2
    val rows = 2
    val size = cols * rows
    
    it("should convert from Array of ints to ArgWritable and back") {
      val expected = Array.fill[Int](size)(1)
      val actual = ArgWritable.fromRasterData(IntArrayRasterData(expected, cols, rows)).toRasterData(TypeInt, cols, rows)
      expected should be(actual.asInstanceOf[IntArrayRasterData].array)
    }

    it("should convert from Array of shorts to ArgWritable and back") {
      val expected = Array.fill[Short](size)(1)
      val actual = ArgWritable.fromRasterData(ShortArrayRasterData(expected, cols, rows)).toRasterData(TypeShort, cols, rows)
      expected should be(actual.asInstanceOf[ShortArrayRasterData].array)
    }

    it("should convert from Array of doubles to ArgWritable and back") {
      val expected = Array.fill[Double](size)(1)
      val actual = ArgWritable.fromRasterData(DoubleArrayRasterData(expected, cols, rows)).toRasterData(TypeDouble, cols, rows)
      expected should be(actual.asInstanceOf[DoubleArrayRasterData].array)
    }

    it("should convert from Array of floats to ArgWritable and back") {
      val expected = Array.fill[Float](size)(1)
      val actual = ArgWritable.fromRasterData(FloatArrayRasterData(expected, cols, rows)).toRasterData(TypeFloat, cols, rows)
      expected should be(actual.asInstanceOf[FloatArrayRasterData].array)
    }

    it("should convert from Array of bytes to ArgWritable and back") {
      val expected = Array.fill[Byte](size)(1)
      val actual = ArgWritable.fromRasterData(ByteArrayRasterData(expected, cols, rows)).toRasterData(TypeByte, cols, rows)
      expected should be(actual.asInstanceOf[ByteArrayRasterData].array)
    }

    it("should convert from Array of bytes (actually, bit masks) to ArgWritable and back") {
      val expected = Array.fill[Byte](size)(1)
      
      // bit mask length is 8x4 since there are 4 bytes of length 8 bits each
      val cols = 8
      val rows = 4
      
      val actual = ArgWritable.fromRasterData(BitArrayRasterData(expected, cols, rows)).toRasterData(TypeBit, cols, rows)
      expected should be(actual.asInstanceOf[BitArrayRasterData].array)
    }
  }
}