package geotrellis.source

import geotrellis._
import geotrellis.testutil._
import geotrellis.process._
import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import geotrellis.raster._
import geotrellis.raster.op._
import geotrellis.statistics._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class DataSourceSpec extends FunSpec 
                        with ShouldMatchers 
                        with TestServer 
                        with RasterBuilders {
  def getRasterSource = 
    RasterSource("mtsthelens_tiled_cached")

  def getSmallRasterSource =
    RasterSource("quad_tiled")

  describe("RasterSource") {
    it("should print history") { 
      val r1 = RasterSource("quad_tiled")
      val r2 = RasterSource("quad_tiled2")
      getSource(r1 + r2) match {
        case Complete(value,success) =>
          println(success.toString)
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }

    it("should return a RasterSource when possible") { 
      val d1 = getRasterSource

      val d2:RasterSource = d1.localAdd(3)
      val d3:RasterSource  = d2 mapOp(local.Add(_, 3))
      val d4:RasterSource = d3 map(r => r.map(z => z + 3))
      val d5:DataSource[Int,Seq[Int]] = d3 map(r => r.findMinMax._2)
      
      val result1 = runSource(d1)
      val result2 = runSource(d2)
      val result3 = runSource(d3)
      val result4 = runSource(d4)
      val result5 = runSource(d5)

      result1.get(100,100) should be (3233)
      result2.get(100,100) should be (3236)
      result3.get(100,100) should be (3239)
      result4.get(100,100) should be (3242)
      result5.head should be (6026)
    }

    it("should handle a histogram result") {
      val d = getRasterSource

      val hist = d.histogram
      val hist2:DataSource[Histogram,Histogram] = d.map( (h:Raster) => FastMapHistogram() )
      case class MinFromHistogram(h:Op[Histogram]) extends Op1(h)({
        (h) => Result(h.getMinValue)
      })

      case class FindMin(ints:Op[Seq[Int]]) extends Op1(ints)({
        (ints) => Result(ints.reduce(math.min(_,_)))
      })

      val ints:DataSource[Int,Seq[Int]] = hist.mapOp(MinFromHistogram(_))
     
      val seqIntVS:ValueDataSource[Seq[Int]] = ints.converge

      val intVS:ValueDataSource[Int] = seqIntVS.map( seqInt => seqInt.reduce(math.min(_,_)))
      val intVS2 = ints.reduce(math.min(_,_))

      val histogramResult = runSource(hist)
      val intsResult = runSource(ints)

      val intResult = runSource(intVS)
      val directIntResult = runSource(intVS2)

      histogramResult.getMinValue should be (2231)
      histogramResult.getMaxValue should be (8367)
      intsResult.length should be (12)
      intResult should be (directIntResult)
      
    }

    it("should handle combine") {
      val d = getRasterSource
      val d2 = getRasterSource

      val combineDS:RasterSource = d.combine(d2)(_+_)
      val initial = runSource(d)
      val result = runSource(combineDS)

      result.get(3,3) should be (initial.get(3,3) * 2)
    }
  }
}
