package geotrellis.vector.methods

import geotrellis.vector._
import geotrellis.util.MethodExtensions

trait ExtraMultiPolygonMethods extends MethodExtensions[MultiPolygon] {
  def polygons: Array[Polygon] = {
    for (i <- 0 until self.getNumGeometries) yield {
      self.getGeometryN(i).copy.asInstanceOf[Polygon]
    }
  }.toArray

  def typedIntersection(p: Point): PointOrNoResult = self.intersection(p)
  def typedIntersection(mp: MultiPoint): MultiPointAtLeastOneDimensionIntersectionResult = self.intersection(mp)
  def typedIntersection(l: LineString): OneDimensionAtLeastOneDimensionIntersectionResult = self.intersection(l)
  def typedIntersection(ml: MultiLineString): OneDimensionAtLeastOneDimensionIntersectionResult = self.intersection(ml)
  def typedIntersection(p: Polygon): TwoDimensionsTwoDimensionsIntersectionResult = self.intersection(p)
  def typedIntersection(mp: MultiPolygon): TwoDimensionsTwoDimensionsIntersectionResult = self.intersection(mp)
  def typedIntersection(ex: Extent): TwoDimensionsTwoDimensionsIntersectionResult = self.intersection(ex.toPolygon)

  def -(p: Point): MultiPolygonXDifferenceResult = self.difference(p)
  def -(mp: MultiPoint): MultiPolygonXDifferenceResult = self.difference(mp)
  def -(l: LineString): MultiPolygonXDifferenceResult = self.difference(l)
  def -(ml: MultiLineString): MultiPolygonXDifferenceResult = self.difference(ml)
  def -(p: Polygon): TwoDimensionsTwoDimensionsDifferenceResult = self.difference(p)
  def -(mp: MultiPolygon): TwoDimensionsTwoDimensionsDifferenceResult = self.difference(mp)

  def normalized(): MultiPolygon = {
    val res = self.copy.asInstanceOf[MultiPolygon]
    res.normalize
    res
  }
}
