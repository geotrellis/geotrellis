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

package geotrellis.engine.logic

import geotrellis.engine._

import org.scalatest._

class CollectSpec extends FunSpec 
                     with TestEngine
                     with Matchers {
  describe("Collect") {
    it("should take a Seq of Op[Int]'s and turn it into a Seq[Int]") {
      val seq = Seq(Literal(1),Literal(2),Literal(3))
      val result = get(Collect(seq))
      result should be (Seq(1,2,3))
    }
  }

  describe("CollectMap") {
    it("should take a Map[String,Op[Int]] and turn it into a Map[String,Int]") {
      val map = Map("one" -> Literal(1),"two" -> Literal(2),"three" -> Literal(3))
      val result = get(Collect(map))
      result should be (Map("one" -> 1,"two" -> 2,"three" -> 3))
    }
  }
}
