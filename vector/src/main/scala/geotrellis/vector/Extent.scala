package geotrellis.vector

import geotrellis.vector.reproject._
import GeomFactory._
import geotrellis.proj4.CRS

import com.vividsolutions.jts.{geom => jts}

case class ExtentRangeError(msg:String) extends Exception(msg)

object Extent {
  def apply(env: jts.Envelope): Extent =
    Extent(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)

  /** Parses a string in the format "xmin,ymin,xmax,ymax" form, e.g.
    * 100.00,600.00,300.00,800.00
    */
  def fromString(s:String) = {
    val Array(xmin,ymin,xmax,ymax) = s.split(",").map(_.toDouble)
    Extent(xmin,ymin,xmax,ymax)
  }

  implicit def toPolygon(extent: Extent): Polygon =
    extent.toPolygon

  implicit def jts2Extent(env: jts.Envelope): Extent =
    Extent(env)
}

case class ProjectedExtent(extent: Extent, crs: CRS) {
  def reproject(dest: CRS): Extent = 
    extent.reproject(crs, dest)
}

object ProjectedExtent {
  implicit def fromTupleA(tup: (Extent, CRS)):ProjectedExtent = ProjectedExtent(tup._1, tup._2)
  implicit def fromTupleB(tup: (CRS, Extent)):ProjectedExtent = ProjectedExtent(tup._2, tup._1)
}

/**
 * An Extent represents a rectangular region of geographic space (with a
 * particular projection). It is expressed in map coordinates.
 */
case class Extent(xmin: Double, ymin: Double, xmax: Double, ymax: Double) extends Geometry {

  // Validation: Do not accept extents min values greater than max values.
  if (xmin > xmax) { throw ExtentRangeError(s"Invalid Extent: xmin must be less than xmax (xmin=$xmin, xmax=$xmax)")  }
  if (ymin > ymax) { throw ExtentRangeError(s"Invalid Extent: ymin must be less than ymax (ymin=$ymin, ymax=$ymax)") }

  def jtsGeom =
    factory.createPolygon(
      factory.createLinearRing(
        Array(
          new jts.Coordinate(xmin, ymax),
          new jts.Coordinate(xmax, ymax),
          new jts.Coordinate(xmax, ymin),
          new jts.Coordinate(xmin, ymin),
          new jts.Coordinate(xmin, ymax)
        )
      ),
      null
    )

  override def isValid: Boolean = true
  override def centroid: PointOrNoResult = PointResult(center)
  override def envelope: Extent = this
  override def interiorPoint: PointOrNoResult = PointResult(center)

  val width = xmax - xmin
  val height = ymax - ymin

  def min = Point(xmin, ymin)
  def max = Point(xmax, ymax)

  /**
   * The SW corner (xmin, ymin) as a Point.
   */
  def southWest = Point(xmin, ymin)

  /**
   * The SE corner (xmax, ymin) as a Point.
   */
  def southEast = Point(xmax, ymin)
 
  /**
   * The NE corner (xmax, ymax) as a Point.
   */
  def northEast = Point(xmax, ymax)

  /**
   * The NW corner (xmin, ymax) as a Point.
   */
  def northWest = Point(xmin, ymax)

  def area = width * height
  def minExtent = if(width < height) width else height
  def maxExtent = if(width > height) width else height

  def center: Point = 
    Point((xmin + xmax) / 2.0, (ymin + ymax) / 2.0)

  def intersects(other: Extent): Boolean =
    !(other.xmax < xmin ||
      other.xmin > xmax) &&
    !(other.ymax < ymin ||
      other.ymin > ymax)

  def intersects(p: Point): Boolean =
    intersects(p.x, p.y)

  def intersects(x: Double, y: Double): Boolean =
    x >= xmin && x <= xmax && y >= ymin && y <= ymax

  def contains(other: Extent): Boolean =
    other.xmin >= xmin &&
    other.ymin >= ymin &&
    other.xmax <= xmax &&
    other.ymax <= ymax

  /**
    * Tests if the given point lies in or on the envelope.
    * 
    * @note Note that this is the same definition as the SFS <tt>contains</tt>,
    *       which is unlike the JTS Envelope.contains, which would include the 
    *       envelope boundary.
    */
  def contains(p: Point): Boolean =
    contains(p.x, p.y)

  /**
    * Tests if the given point lies in or on the envelope.
    * 
    * @note Note that this is the same definition as the SFS <tt>contains</tt>,
    *       which is unlike the JTS Envelope.contains, which would include the 
    *       envelope boundary.
    */
  def contains(x: Double, y: Double): Boolean =
    x > xmin && x < xmax && y > ymin && y < ymax

  def covers(other: Extent): Boolean =
    intersects(other)

  def covers(p: Point): Boolean =
    covers(p.x, p.y)

  def covers(x: Double, y: Double): Boolean =
    intersects(x, y)

  def distance(other: Extent): Double =
    if(intersects(other)) 0
    else {
      val dx = 
        if(xmax < other.xmin)
          other.xmin - xmax
        else if(xmin > other.xmax)
          xmin - other.xmax
        else
          0.0
    
        val dy = 
          if(ymax < other.ymin)
            other.ymin - ymax
          else if(ymin > other.ymax) 
            ymin - other.ymax
          else
            0.0

        // if either is zero, the envelopes overlap either vertically or horizontally
        if(dx == 0.0) 
          dy
        else if(dy == 0.0) 
          dx
        else
          math.sqrt(dx * dx + dy * dy)
    }

  def intersection(other: Extent): Option[Extent] = {
    val xminNew = if(xmin > other.xmin) xmin else other.xmin
    val yminNew = if(ymin > other.ymin) ymin else other.ymin
    val xmaxNew = if(xmax < other.xmax) xmax else other.xmax
    val ymaxNew = if(ymax < other.ymax) ymax else other.ymax 

    if(xminNew <= xmaxNew && yminNew <= ymaxNew) {
      Some(Extent(xminNew, yminNew, xmaxNew, ymaxNew))
    } else { None }
  }

  def &(other: Extent): Option[Extent] = 
    intersection(other)

  def buffer(d: Double): Extent =
    Extent(xmin - d, ymin - d, xmax + d, ymax + d)

  /**
   * Orders two bounding boxes by their (geographically) lower-left corner. The bounding box
   * that is further south (or west in the case of a tie) comes first.
   *
   * If the lower-left corners are the same, the upper-right corners are
   * compared. This is mostly to assure that 0 is only returned when the
   * extents are equal.
   *
   * Return type signals:
   *
   *   -1 this bounding box comes first
   *    0 the bounding boxes have the same lower-left corner
   *    1 the other bounding box comes first
   */
  def compare(other: Extent): Int = {
    var cmp = ymin compare other.ymin
    if (cmp != 0) return cmp

    cmp = xmin compare other.xmin
    if (cmp != 0) return cmp

    cmp = ymax compare other.ymax
    if (cmp != 0) return cmp

    xmax compare other.xmax
  }

  /**
   * Return a the smallest extent that contains this extent and the provided
   * extent. This is provides a union of the two extents.
   */
  def combine(other:Extent): Extent =
    Extent(
      if(xmin < other.xmin) xmin else other.xmin,
      if(ymin < other.ymin) ymin else other.ymin,
      if(xmax > other.xmax) xmax else other.xmax,
      if(ymax > other.ymax) ymax else other.ymax
    )

  def expandToInclude(other: Extent): Extent =
    combine(other)

  def expandToInclude(p: Point): Extent =
    expandToInclude(p.x, p.y)

  def expandToInclude(x: Double, y: Double): Extent =
    Extent(
      if(xmin < x) xmin else x,
      if(ymin < y) ymin else y,
      if(xmax > x) xmax else x,
      if(ymax > y) ymax else y
    )

  def expandBy(distance: Double): Extent = 
    expandBy(distance, distance)


  def expandBy(deltaX: Double, deltaY: Double): Extent =
    Extent(
      xmin - deltaX,
      ymin - deltaY,
      xmax + deltaX,
      ymax + deltaY
    )

  def translate(deltaX: Double, deltaY: Double): Extent =
    Extent(
      xmin + deltaX,
      ymin + deltaY,
      xmax + deltaX,
      ymin + deltaY
    )

  def toPolygon(): Polygon = 
    Polygon( Line((xmin, ymin), (xmin, ymax), (xmax, ymax), (xmax, ymin), (xmin, ymin)) )

  override
  def equals(o: Any): Boolean =
    o match {
      case other: Extent => 
        xmin == other.xmin && ymin == other.ymin &&
        xmax == other.xmax && ymax == other.ymax
      case _ => false
  }

  override
  def hashCode(): Int = (xmin, ymin, xmax, ymax).hashCode

  override def toString = s"Extent($xmin, $ymin, $xmax, $ymax)"
}
