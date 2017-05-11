package geotrellis.vector.io.json

import geotrellis.vector._

import spray.json._

/** Abstract trait for various implementations of Coordinate Reference System values */
sealed abstract trait CRS {
  def toJson:JsValue

  def addTo(json: JsValue): JsValue =  json match {
    case JsObject(fields) =>
      JsObject(fields + ("crs" -> toJson))
    case _ => throw new SerializationException("JsObject required to add CRS")
  }
}

/** A blank CRS field (will not be associated with GeoJSON object) */
object BlankCRS extends CRS{
  override def toJson: JsValue = JsNull
  override def addTo(json: JsValue) = json
}

/** A CRS object which indicates a coordinate reference system by name.
  *
  * @param name must be a string identifying a coordinate reference system.
  * @note OGC CRS URNs such as "urn:ogc:def:crs:OGC:1.3:CRS84" shall be
  *       preferred over legacy identifiers such as "EPSG:4326"
  */
case class NamedCRS(name: String) extends CRS {
  override def toJson: JsValue =
    JsObject(
      "type" -> JsString("name"),
      "properties" -> JsObject("name" -> JsString(name))
    )
}

/** A CRS object with a link to CRS parameters on the Web.
  *
  * @param href must be a dereferenceable URI.
  * @param crsType must be a string that hints at the format
  *        used to represent CRS parameters at the provided URI.
  * @note  Suggested values are: "proj4", "ogcwkt", "esriwkt", others can be used:
  */
case class LinkedCRS(href: String, crsType: String = "") extends CRS {
  override def toJson: JsValue =
    JsObject(
      "type" -> JsString("link"),
      "properties" -> JsObject(
        "href" -> JsString(href),
        "type" -> (if (crsType == "") JsNull else JsString(crsType))
      )
    )
}

/** Used as a named tuple to extract and insert CRS field in GeoJSON objects */
case class WithCrs[T](t: T, crs: CRS)
