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

package geotrellis.spark.io.hadoop.reader

import geotrellis.spark.TestEnvironment
import geotrellis.spark.tiling._
import geotrellis.spark.io.hadoop.formats._
import geotrellis.spark.testfiles.AllOnesTestFile

import org.scalatest._

class RasterReaderSpec extends FunSpec with TestEnvironment with Matchers {
  def tileSpans =
    AllOnesTestFile(inputHome, conf).metaData.tileIds.spans

  def tileIds = AllOnesTestFile(inputHome, conf).metaData.tileIds

  def read(start: Long, end: Long): Int = {
    val allOnes = AllOnesTestFile(inputHome, conf)
    val reader = RasterReader(allOnes.path, conf, start, end)
    val numEntries = reader.count(_ => true)
    reader.close()
    numEntries
  }

  describe("RasterReader") {
    it("should retrieve all entries") {
      read(tileIds.min, tileIds.max) should be(12)
    }

    it("should retrieve all entries when no range is specified") {
      read(Long.MinValue, Long.MaxValue) should be(12)
    }

    it("should handle a non-existent start and first row end") {
      val tileId = tileSpans.head._2
      read(0, tileId) should be(3)
    }

    it("should be able to skip a partition") {
      val tileId = tileSpans.last._1
      read(tileId, Long.MaxValue) should be(3)
    }

    it("should be handle start=end") {
      val tileId = tileIds(10)
      read(tileId, tileId) should be(1)
    }
  }
}
