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

package geotrellis.raster.render

import geotrellis.raster._
import geotrellis.raster.histogram.Histogram

import scala.collection.mutable

sealed abstract class ColorMapType

case object GreaterThan extends ColorMapType
case object GreaterThanOrEqualTo extends ColorMapType
case object LessThan extends ColorMapType
case object LessThanOrEqualTo extends ColorMapType
case object Exact extends ColorMapType

case class ColorMapOptions(
  colorMapType: ColorMapType,
  /** Rgba value for NODATA */
  noDataColor: Int = 0x00000000,
  /** Rgba value for data that doesn't fit the map */
  noMapColor: Int = 0x00000000,
  /** Set to true to throw exception on unmappable variables */
  strict: Boolean = false
)

object ColorMapOptions {
  val Default = ColorMapOptions(LessThan)

  def apply(nd: Int): ColorMapOptions =
    ColorMapOptions(LessThan, nd)

  implicit def colorMapTypeToOptions(colorMapType: ColorMapType): ColorMapOptions =
    ColorMapOptions(colorMapType)
}

object ColorMap {
  def apply(breaksToColors: Map[Int, Int]): IntColorMap =
    IntColorMap(breaksToColors)

  def apply(breaksToColors: Map[Int, Int],
            options: ColorMapOptions): IntColorMap =
    IntColorMap(breaksToColors, options)

  def apply(breaksToColors: Map[Double, Int]): DoubleColorMap =
    DoubleColorMap(breaksToColors)

  def apply(breaksToColors: Map[Double, Int], options: ColorMapOptions): DoubleColorMap =
    DoubleColorMap(breaksToColors, options)

  def apply(colorBreaks: ColorBreaks): ColorMap =
    apply(colorBreaks, ColorMapOptions.Default)

  def apply(colorBreaks: ColorBreaks, options: ColorMapOptions): ColorMap =
    colorBreaks.toColorMap(options)

  def apply(breaks: Array[Int], color: Array[Int]): IntColorMap =
    apply(breaks, color, ColorMapOptions.Default)

  def apply(breaks: Array[Int], colors: Array[Int], options: ColorMapOptions): IntColorMap = {
    val breaksToColors: Map[Int, Int] =
      breaks.zip(colors)
            .toMap

    apply(breaksToColors, options)
  }

  def apply(breaks: Array[Double], color: Array[Int]): DoubleColorMap =
    apply(breaks, color, ColorMapOptions.Default)

  def apply(breaks: Array[Double], colors: Array[Int], options: ColorMapOptions): DoubleColorMap = {
    val breaksToColors: Map[Double, Int] =
      breaks.zip(colors)
            .toMap

    apply(breaksToColors, options)
  }
}

trait ColorMap {
  def colors: Array[Int]
  val options: ColorMapOptions

  private var _opaque = true
  private var _grey = true
  private var _colorsChecked = false

  private def checkColors() = {
    var opaque = true
    var grey = true
    var i = 0
    while (i < colors.length) {
      val c = colors(i)
      opaque &&= Color.isOpaque(c)
      grey &&= Color.isGrey(c)
      i += 1
    }
    _colorsChecked = true
  }

  def opaque =
    if(_colorsChecked) { _opaque }
    else {
      checkColors()
      _opaque
    }

  def grey =
    if(_colorsChecked) { _grey }
    else {
      checkColors()
      _grey
    }

  def render(r: Tile): Tile

  def cache(h: Histogram): ColorMap
}

case class IntColorMap(breaksToColors: Map[Int, Int],
                      options: ColorMapOptions = ColorMapOptions.Default) extends ColorMap {
  val orderedBreaks: Array[Int] =
    options.colorMapType match {
      case LessThan | LessThanOrEqualTo =>
        breaksToColors.keys.toArray.sorted
      case GreaterThan | GreaterThanOrEqualTo =>
        breaksToColors.keys.toArray.sorted.reverse
      case Exact =>
        breaksToColors.keys.toArray
    }

  val orderedColors: Array[Int] = orderedBreaks.map(breaksToColors(_))
  lazy val colors = orderedColors

  val zCheck: (Int, Int) => Boolean =
    options.colorMapType match {
      case LessThan =>
        { (z: Int, i: Int) => z > orderedBreaks(i) }
      case LessThanOrEqualTo =>
        { (z: Int, i: Int) => z >= orderedBreaks(i) }
      case GreaterThan =>
        { (z: Int, i: Int) => z < orderedBreaks(i) }
      case GreaterThanOrEqualTo =>
        { (z: Int, i: Int) => z <= orderedBreaks(i) }
      case Exact =>
        { (z: Int, i: Int) => z != orderedBreaks(i) }
    }

  val len = orderedBreaks.length

  def apply(z: Int) = {
    if(isNoData(z)) { options.noDataColor }
    else {
      var i = 0
      while(i < len && zCheck(z, i)) { i += 1 }
      if(i == len){
        if(options.strict) {
          sys.error(s"Value $z did not have an associated color and break")
        } else {
          options.noMapColor
        }
      } else {
        orderedColors(i)
      }
    }
  }

  def render(r: Tile) =
      r.convert(TypeByte).map { z => apply(z) }

  def cache(h: Histogram): ColorMap = {
    val ch = h.mutable
    h.foreachValue(z => ch.setItem(z, apply(z)))
    CachedColorMap(orderedColors, options, ch)
  }
}

case class CachedColorMap(colors: Array[Int], options: ColorMapOptions, h: Histogram)
  extends ColorMap with Function1[Int, Int] {
  final val noDataColor = options.noDataColor
  def render(r: Tile) =
    r.map(this)
  final def apply(z: Int) = { if(isNoData(z)) noDataColor else h.getItemCount(z) }
  def cache(h: Histogram) = this
}

case class DoubleColorMap(breaksToColors: Map[Double, Int],
                          options: ColorMapOptions = ColorMapOptions.Default) extends ColorMap {
  lazy val colors = breaksToColors.values.toArray
  val orderedBreaks: Array[Double] =
    options.colorMapType match {
      case LessThan | LessThanOrEqualTo =>
        breaksToColors.keys.toArray.sorted
      case GreaterThan | GreaterThanOrEqualTo =>
        breaksToColors.keys.toArray.sorted.reverse
      case Exact =>
        breaksToColors.keys.toArray
    }

  val zCheck: (Double, Int) => Boolean =
    options.colorMapType match {
      case LessThan =>
        { (z: Double, i: Int) => z > orderedBreaks(i) }
      case LessThanOrEqualTo =>
        { (z: Double, i: Int) => z >= orderedBreaks(i) }
      case GreaterThan =>
        { (z: Double, i: Int) => z < orderedBreaks(i) }
      case GreaterThanOrEqualTo =>
        { (z: Double, i: Int) => z <= orderedBreaks(i) }
      case Exact =>
        { (z: Double, i: Int) => z != orderedBreaks(i) }
    }

  val len = orderedBreaks.length

  def apply(z: Double) = {
    if(isNoData(z)) { options.noDataColor }
    else {
      var i = 0
      while(i < len && zCheck(z, i)) { i += 1 }

      if(i == len){
        if(options.strict) {
          sys.error(s"Value $z did not have an associated color and break")
        } else {
          options.noMapColor
        }
      } else {
        breaksToColors(orderedBreaks(i))
      }
    }
  }

  def render(r: Tile) =
      r.mapDouble { z => apply(z) }

  def cache(h: Histogram): ColorMap = {
    val ch = h.mutable

    h.foreachValue(z => ch.setItem(z, apply(z)))
    val cs = colors
    val opts = options

    new ColorMap {
      lazy val colors = cs
      val options = opts
      def render(r: Tile) =
        r.map { z => if(z == NODATA) options.noDataColor else ch.getItemCount(z) }
      def cache(h: Histogram) = this
    }
  }
}
