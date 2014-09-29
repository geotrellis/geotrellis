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

import geotrellis.spark.io.hadoop._
import geotrellis.spark.rdd._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class TestFiles(val path: Path, conf: Configuration) {
  val metaData =
    HadoopUtils.readLayerMetaData(path, conf)

  def tileCount =
    metaData.tileIds.size
}

object AllOnesTestFile {
  def apply(prefix: Path, conf: Configuration) =
    new TestFiles(
      new Path(prefix, "all-ones/10"),
      conf
    )
}

object AllTwosTestFile {
  def apply(prefix: Path, conf: Configuration) =
    new TestFiles(
      new Path(prefix, "all-twos/10"),
      conf
    )
}

object AllHundredsTestFile {
  def apply(prefix: Path, conf: Configuration) =
    new TestFiles(
      new Path(
        prefix, "all-hundreds/10"),
      conf
    )
}

object IncreasingTestFile {
  def apply(prefix: Path, conf: Configuration) =
    new TestFiles(
      new Path(prefix, "increasing/10"),
      conf
    )
}

object DecreasingTestFile {
  def apply(prefix: Path, conf: Configuration) =
    new TestFiles(
      new Path(prefix, "decreasing/10"),
      conf
    )
}

object EveryOtherUndefinedTestFile {
  def apply(prefix: Path, conf: Configuration) =
    new TestFiles(
      new Path(prefix, "every-other-undefined/10"),
      conf
    )
}

object EveryOther0Point99Else1Point01TestFile {
  def apply(prefix: Path, conf: Configuration) =
    new TestFiles(
      new Path(prefix, "every-other-0.99-else-1.01/10"),
      conf
    )
}

object EveryOther1ElseMinus1TestFile {
  def apply(prefix: Path, conf: Configuration) =
    new TestFiles(
      new Path(prefix, "every-other-1-else-1/10"),
      conf
    )
}
