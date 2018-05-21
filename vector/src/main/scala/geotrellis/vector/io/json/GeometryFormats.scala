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

package geotrellis.vector.io.json

import io.circe._
import io.circe.syntax._
import cats.syntax.either._

import geotrellis.vector._

/** A trait that implements Circe Encoders and Decoders for Geometry objects.
  * @note Import or extend this object directly to use them with default circe (un)marshaller
  */
trait GeometryFormats {
  /** Writes point to JsArray as [x, y] */
  private def writePointCoords(point: Point): Json =
    Vector(point.x.asJson, point.y.asJson).asJson

  /** JsArray of [x, y] arrays */
  private def writeLineCoords(line: Line): Json =
    line.points.map(writePointCoords).toVector.asJson

  /** JsArray of Lines for the polygin, first line is exterior, rest are holes*/
  private def writePolygonCoords(polygon: Polygon): Json =
    (writeLineCoords(polygon.exterior) +: polygon.holes.map(writeLineCoords).toVector).asJson

  /** Reads Point from JsArray of [x, y] */
  private def readPointCoords(value: Json): Point = value.as[Vector[Double]] match {
    case Right(Vector(x, y)) => Point(x, y)
    case Right(Vector(x, y, _)) => Point(x, y)
    case _ => throw new Exception("Point [x,y] or [x,y,_] coordinates expected")
  }

  /** Reads Line as JsArray of [x, y] point elements */
  private def readLineCoords(value: Json): Line = value.asArray match {
    case Some(arr) => Line(arr.map(readPointCoords))
    case _ => throw new Exception("Line coordinates array expected")
  }

  /** Reads Polygon from JsArray containg Lines for polygon */
  private def readPolygonCoords(value: Json): Polygon = value.asArray match {
    case Some(arr) =>
      val lines: Vector[Line] = 
        arr
           .map(readLineCoords)
           .map(_.closed)

      Polygon(lines.head, lines.tail.toSet)
    case _ => throw new Exception("Polygon coordinates array expected")
  }

  implicit lazy val pointEncoder: Encoder[Point] =
    Encoder.encodeJson.contramap[Point] { obj =>
      Json.obj(
        "type" -> "Point".asJson,
        "coordinates" -> writePointCoords(obj)
      )
    }

  implicit lazy val pointDecoder: Decoder[Point] =
    Decoder.decodeJson.emap { json: Json =>
      val c = json.hcursor
      c.downField("type").as[String].flatMap {
        case "Point" => c.downField("coordinates").focus.map(readPointCoords).toRight("Point geometry expected")
        case "Feature" => pointDecoder(unwrapFeature(json).hcursor)
        case _ => Left("Point geometry expected")
      }.leftMap(_ => "Point geometry expected")
    }

  implicit lazy val lineEncoder: Encoder[Line] =
    Encoder.encodeJson.contramap[Line] { obj =>
      Json.obj(
        "type" -> "LineString".asJson,
        "coordinates" -> writeLineCoords(obj)
      )
    }

  implicit lazy val lineDecoder: Decoder[Line] =
    Decoder.decodeJson.emap { json: Json =>
      val c = json.hcursor
      c.downField("type").as[String].flatMap {
        case "LineString" => c.downField("coordinates").focus.map(readLineCoords).toRight("LineString geometry expected")
        case "Feature" => lineDecoder(unwrapFeature(json).hcursor)
        case _ => Left("LineString geometry expected")
      }.leftMap(_ => "LineString geometry expected")
    }

  implicit lazy val polygonEncoder: Encoder[Polygon] =
    Encoder.encodeJson.contramap[Polygon] { obj =>
      Json.obj(
        "type" -> "Polygon".asJson,
        "coordinates" -> writePolygonCoords(obj)
      )
    }

  implicit lazy val polygonDecoder: Decoder[Polygon] =
    Decoder.decodeJson.emap { json: Json =>
      val c = json.hcursor
      c.downField("type").as[String].flatMap {
        case "Polygon" => c.downField("coordinates").focus.map(readPolygonCoords).toRight("Polygon geometry expected")
        case "Feature" => polygonDecoder(unwrapFeature(json).hcursor)
        case _ => Left("Polygon geometry expected")
      }.leftMap(_ => "Polygon geometry expected")
    }

  /** Extent gets it's own non-GeoJson JSON representation.
    * If you're using the Extent as a geometry, however, it gets converted
    * to a Polygon and written out in GeoJson as a Polygon
    */
  implicit lazy val extentEncoder: Encoder[Extent] =
    new Encoder[Extent] {
      final def apply(extent: Extent): Json =
        List(extent.xmin, extent.ymin, extent.xmax, extent.ymax).asJson
    }
  implicit lazy val extentDecoder: Decoder[Extent] =
    Decoder[Json] emap { value =>
      value.as[List[Double]].map { case List(xmin, ymin, xmax, ymax) =>
        Extent(xmin, ymin, xmax, ymax)
      }.leftMap(_ => s"Extent [xmin,ymin,xmax,ymax] expected: $value")
    }

  implicit lazy val multiPointEncoder: Encoder[MultiPoint] =
    Encoder.encodeJson.contramap[MultiPoint] { obj =>
      Json.obj(
        "type" -> "MultiPolygon".asJson,
        "coordinates" -> obj.points.map(writePointCoords).asJson
      )
    }

  object extentListEncoder {
    def write(extent: Extent) =List(extent.xmin, extent.ymin, extent.xmax, extent.ymax).asJson
  }

  implicit lazy val multiPointDecoder: Decoder[MultiPoint] =
    Decoder.decodeJson.emap { json: Json =>
      val c = json.hcursor
      c.downField("type").as[String].flatMap {
        case "MultiPoint" => c.downField("coordinates").focus.map(json => MultiPoint(json.asArray.toVector.flatten.map(readPointCoords))).toRight("MultiPoint geometry expected")
        case "Feature" => multiPointDecoder(unwrapFeature(json).hcursor)
        case _ => Left("MultiPoint geometry expected")
      }.leftMap(_ => "MultiPoint geometry expected")
    }

  implicit lazy val multiLineEncoder: Encoder[MultiLine] =
    Encoder.encodeJson.contramap[MultiLine] { obj =>
      Json.obj(
        "type" -> "MultiLine".asJson,
        "coordinates" -> obj.lines.map(writeLineCoords).toVector.asJson
      )
    }

  implicit lazy val multiLineDecoder: Decoder[MultiLine] =
    Decoder.decodeJson.emap { json: Json =>
      val c = json.hcursor
      c.downField("type").as[String].flatMap {
        case "MultiLineString" => c.downField("coordinates").focus.map(json => MultiLine(json.asArray.toVector.flatten.map(readLineCoords))).toRight("MultiLine geometry expected")
        case "Feature" => multiLineDecoder(unwrapFeature(json).hcursor)
        case _ => Left("MultiLine geometry expected")
      }.leftMap(_ => "MultiLine geometry expected")
    }


  implicit lazy val multiPolygonEncoder: Encoder[MultiPolygon] =
    Encoder.encodeJson.contramap[MultiPolygon] { obj =>
      Json.obj(
        "type" -> "MultiPolygon".asJson,
        "coordinates" -> obj.polygons.map(writePolygonCoords).toVector.asJson
      )
    }

  implicit lazy val multiPolygonDecoder: Decoder[MultiPolygon] =
    Decoder.decodeJson.emap { json: Json =>
      val c = json.hcursor
      c.downField("type").as[String].flatMap {
        case "MultiPolygon" => c.downField("coordinates").focus.map(json => MultiPolygon(json.asArray.toVector.flatten.map(readPolygonCoords))).toRight("MultiPolygon geometry expected")
        case "Feature" => multiPolygonDecoder(unwrapFeature(json).hcursor)
        case _ => Left("MultiPolygon geometry expected")
      }.leftMap(_ => "MultiPolygon geometry expected")
    }

  implicit lazy val geometryCollectionEncoder: Encoder[GeometryCollection] =
    Encoder.encodeJson.contramap[GeometryCollection] { obj =>
      Json.obj(
        "type" -> "GeometryCollection".asJson,
        "geometries" -> Vector(
          obj.points.map(_.asJson),
          obj.lines.map(_.asJson),
          obj.polygons.map(_.asJson),
          obj.multiPoints.map(_.asJson),
          obj.multiLines.map(_.asJson),
          obj.multiPolygons.map(_.asJson),
          obj.geometryCollections.map(_.asJson)
        ).asJson
      )
    }

  implicit lazy val geometryCollectionDecoder: Decoder[GeometryCollection] =
    Decoder.decodeJson.emap { json: Json =>
      val c = json.hcursor
      c.downField("type").as[String].flatMap {
        case "GeometryCollection" => c.downField("geometries").focus.map(json => GeometryCollection(json.asArray.toVector.flatten.flatMap(j => geometryDecoder(j.hcursor).toOption))).toRight("MultiPolygon geometry expected")
        case "Feature" => geometryCollectionDecoder(unwrapFeature(json).hcursor)
        case _ => Left("GeometryCollection geometry expected")
      }.leftMap(_ => "GeometryCollection geometry expected")
    }

  implicit lazy val geometryEncoder: Encoder[Geometry] =
    Encoder.encodeJson.contramap[Geometry] {
      case geom: Point => geom.asJson
      case geom: Line => geom.asJson
      case geom: Polygon => geom.asJson
      case geom: Extent => geom.toPolygon.asJson
      case geom: MultiPolygon => geom.asJson
      case geom: MultiPoint => geom.asJson
      case geom: MultiLine => geom.asJson
      case geom: GeometryCollection => geom.asJson
      case geom => throw new Exception(s"Unknown Geometry type ${geom.getClass.getName}: $geom")
    }

  implicit lazy val geometryDecoder: Decoder[Geometry] =
    Decoder.decodeJson.emap { json: Json =>
      val c = json.hcursor
      c.downField("type").as[String].flatMap {
        case "Feature" => geometryDecoder(unwrapFeature(json).hcursor)
        case "Point" => json.as[Point]
        case "LineString" => json.as[Line]
        case "Polygon" => json.as[Polygon]
        case "MultiPolygon" => json.as[MultiPolygon]
        case "MultiPoint" => json.as[MultiPoint]
        case "MultiLineString" => json.as[MultiLine]
        case "GeometryCollection" => json.as[GeometryCollection]
        case t => Left(s"Unknown Geometry type: $t")
      }.leftMap(_ => "Geometry expected")
    }

  /** Unwrap feature geometry (ignoring its properties) */
  private def unwrapFeature(value: Json): Json = {
    val c = value.hcursor
    (c.downField("type").as[String], c.downField("geometry").focus) match {
      case (Right("Feature"), Some(geom)) => geom
      case _ => value
    }
  }
}

object GeometryFormats extends GeometryFormats
