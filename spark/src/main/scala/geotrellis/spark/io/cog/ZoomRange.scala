/*
 * Copyright 2018 Azavea
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

package geotrellis.spark.io.cog

import io.circe.{KeyDecoder, KeyEncoder}
import io.circe.syntax._
import io.circe.generic.JsonCodec
import io.circe.parser.parse

import cats.syntax.either._

@JsonCodec
case class ZoomRange(minZoom: Int, maxZoom: Int) {
  def isSingleZoom: Boolean = minZoom == maxZoom

  def slug: String = s"${minZoom}_${maxZoom}"

  def zoomInRange(zoom: Int): Boolean =
    zoom >= minZoom && zoom <= maxZoom
}

object ZoomRange {
  implicit def ordering[A <: ZoomRange]: Ordering[A] = Ordering.by(_.maxZoom)

  implicit val zoomRangeKeyDecoder: KeyDecoder[ZoomRange] = new KeyDecoder[ZoomRange] {
    override def apply(key: String): Option[ZoomRange] = parse(key).flatMap(_.as[ZoomRange]).toOption
  }

  implicit val zoomRangeKeyEncoder: KeyEncoder[ZoomRange] = new KeyEncoder[ZoomRange] {
    override def apply(key: ZoomRange): String = key.asJson.noSpaces
  }
}
