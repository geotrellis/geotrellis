package geotrellis.vector

import scala.collection.JavaConversions._

import com.vividsolutions.jts.{geom => jts}
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion

import geotrellis.vector._

trait SeqMethods {

  implicit class SeqLineExtensions(val lines: Traversable[Line]) {

    val ml: MultiLine = MultiLine(lines)

    def unionGeometries() = ml.union
    def intersectionGeometries() = ml.intersection
    def differenceGeometries() = ml.difference
    def symDifferenceGeometries() = ml.symDifference

    def toMultiLine = ml
  }

  implicit class SeqPointExtensions(val points: Traversable[Point]) {

    val mp: MultiPoint = MultiPoint(points)

    def unionGeometries() = mp.union
    def intersectionGeometries() = mp.intersection
    def differenceGeometries() = mp.difference
    def symDifferenceGeometries() = mp.symDifference

    def toMultiPoint = mp
  }

  implicit class SeqPolygonExtensions(val polygons: Traversable[Polygon]) {

    val mp: MultiPolygon = MultiPolygon(polygons)

    def unionGeometries(): TwoDimensionsTwoDimensionsSeqUnionResult =
      if(polygons.isEmpty)
        NoResult
      else
        new CascadedPolygonUnion(polygons.map(geom => geom.jtsGeom).toSeq).union()

    def intersectionGeometries() = mp.intersection
    def differenceGeometries() = mp.difference
    def symDifferenceGeometries() = mp.symDifference

    def toMultiPolygon() = mp
  }

  implicit class SeqMultiLineExtensions(val multilines: Traversable[MultiLine]) {

    val ml: MultiLine = MultiLine(multilines.map(_.lines).flatten)

    def unionGeometries() = ml.union
    def intersectionGeometries() = ml.intersection
    def differenceGeometries() = ml.difference
    def symDifferenceGeometries() = ml.symDifference
  }

  implicit class SeqMultiPointExtensions(val multipoints: Traversable[MultiPoint]) {

    val mp: MultiPoint = MultiPoint(multipoints.map(_.points).flatten)

    def unionGeometries() = mp.union
    def intersectionGeometries() = mp.intersection
    def differenceGeometries() = mp.difference
    def symDifferenceGeometries() = mp.symDifference
  }

  implicit class SeqMultiPolygonExtensions(val multipolygons: Traversable[MultiPolygon]) {

    val mp: MultiPolygon = MultiPolygon(multipolygons.map(_.polygons).flatten)

    def unionGeometries(): TwoDimensionsTwoDimensionsSeqUnionResult =
      if(multipolygons.isEmpty)
        NoResult
      else
        new CascadedPolygonUnion(mp.polygons.map(geom => geom.jtsGeom).toSeq).union

    def intersectionGeometries() = mp.intersection
    def differenceGeometries() = mp.difference
    def symDifferenceGeometries() = mp.symDifference
  }
}
