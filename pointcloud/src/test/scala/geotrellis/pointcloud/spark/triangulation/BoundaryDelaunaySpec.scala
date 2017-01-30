package geotrellis.pointcloud.spark.triangulation

import com.vividsolutions.jts.algorithm.distance.{DistanceToPoint, PointPairDistance}
import com.vividsolutions.jts.geom.Coordinate
import geotrellis.vector.Extent
import geotrellis.vector.triangulation._
import org.scalatest.{FunSpec, Matchers}

import scala.util.Random

class BoundaryDelaunaySpec extends FunSpec with Matchers {

  def randInRange(low: Double, high: Double): Double = {
    val x = Random.nextDouble
    low * (1-x) + high * x
  }

  def randomPoint(extent: Extent): Coordinate = {
    new Coordinate(randInRange(extent.xmin, extent.xmax), randInRange(extent.ymin, extent.ymax))
  }

  def randomizedGrid(n: Int, extent: Extent): Seq[Coordinate] = {
    val xs = (for (i <- 1 to n) yield randInRange(extent.xmin, extent.xmax)).sorted
    val ys = for (i <- 1 to n*n) yield randInRange(extent.ymin, extent.ymax)

    xs.flatMap{ x => {
      val yvals = Random.shuffle(ys).take(n).sorted
      yvals.map{ y => new Coordinate(x, y) }
    }}
  }

  describe("BoundaryDelaunay") {
    it("should take all triangles with circumcircles outside extent") {
      val ex = Extent(0,0,1,1)
      val pts = (for ( i <- 1 to 1000 ) yield randomPoint(ex)).toArray
      val dt = DelaunayTriangulation(pts)
      val bdt = BoundaryDelaunay(dt, ex)
      val bdtTris = bdt.triangles.getTriangles.keys.toSet

      def circumcircleLeavesExtent(tri: Int): Boolean = {
        import dt.navigator._
        implicit val trans = dt.verts.getCoordinate(_)

        val center = Predicates.circleCenter(getDest(tri), getDest(getNext(tri)), getDest(getNext(getNext(tri))))
        val radius = center.distance(trans(getDest(tri)))
        val ppd = new PointPairDistance
        
        DistanceToPoint.computeDistance(ex.toPolygon.jtsGeom, center, ppd)
        ppd.getDistance < radius
      }

      dt.triangles.getTriangles.toSeq.forall{ case (idx, tri) => {
        if (circumcircleLeavesExtent(tri))
          bdtTris.contains(idx)
        else {
          //!bdtTris.contains(idx)
          true
        }
      }} should be (true)
    }

    it("should have sane triangle ordering near boundaries") {
      val pts = randomizedGrid(100, Extent(0,0,1,1)).toArray
      val dt = DelaunayTriangulation(pts, false)
      val bdt = BoundaryDelaunay(dt, Extent(0,0,1,1))

      implicit val trans = { i: Int => pts(i) }
      implicit val nav = bdt.navigator
      import nav._

      var validCW = true
      var e = bdt.boundary
      do {
        var f = e
        do {
          if (rotCWSrc(f) != e)
            validCW = !Predicates.isLeftOf(f, getDest(rotCWSrc(f)))

          f = rotCWSrc(f)
        } while (validCW && f != e)

        e = getNext(e)
      } while (validCW && e != bdt.boundary)

      var validCCW = true
      e = bdt.boundary
      do {
        var f = getFlip(e)
        do {
          if (rotCCWSrc(f) != getFlip(e))
            validCCW = !Predicates.isRightOf(f, getDest(rotCCWSrc(f)))

          f = rotCCWSrc(f)
        } while (validCCW && f != getFlip(e))

        e = getNext(e)
      } while (validCCW && e != bdt.boundary)

      (validCW && validCCW) should be (true)
    }
  }

}
