package geotrellis.raster.op.focal

import geotrellis._
import geotrellis.testutil._
import geotrellis.statistics.op.stat._

import spire.syntax._
import scala.collection.mutable

import org.scalatest.FunSpec

class RegionGroupSpec extends FunSpec
                         with TestServer
                         with RasterBuilders {
  describe("RegionGroup") {
    it("should group regions.") {
      val r = createRaster(
        Array(NODATA,NODATA,NODATA,NODATA,     9,
              NODATA,NODATA,     9,     9,     9,
              NODATA,     9,NODATA,     5,     5,
              NODATA,     9,     9,NODATA,NODATA,
              NODATA,NODATA,     9,     9,NODATA,
                   7,     7,NODATA,     9,     9),
          5, 6
      )
      val regions = run(RegionGroup(r)).raster

      val histogram = run(GetHistogram(regions))
      val count = histogram.getValues.length
      count should be (4)

    }

    it("should group regions on larger example.") {
      val n = NODATA
      val r = createRaster(
        Array( n, 1, 1, n, n, n, n, n, n, n, 7, 7, n, n, n, n, n,
               n, 1, 1, n, n, n, n, n, n, n, 7, 7, n, n, n, n, n,
               n, n, n, 1, 1, n, n, n, n, n, 7, n, n, 7, n, n, n,
               n, n, n, 1, 1, n, n, n, n, n, 7, 7, n, 7, 7, n, n,
               n, n, n, n, n, n, n, n, n, n, n, 7, n, n, 7, n, n,
               n, n, n, n, n, n, n, n, n, n, n, 7, 7, 7, 7, n, n,
               n, n, n, n, n, n, n, n, n, n, n, n, n, 7, n, n, n,
               n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n,
               n, n, n, n, 3, n, n, 3, n, n, n, n, n, n, n, n, n,
               n, n, 3, 3, 3, 3, 3, 3, 3, 3, n, n, n, n, n, n, n,
               n, n, n, n, n, n, 5, n, n, 3, 3, n, n, n, n, 8, 8,
               n, n, n, n, n, n, 5, n, n, n, n, n, n, n, n, 8, n,
               n, n, n, n, n, n, 5, n, n, n, n, n, n, 8, 8, 8, n,
               n, n, n, n, n, n, 5, 5, n, n, n, n, n, 8, n, n, n,
               n, n, n, 5, 5, n, 5, n, n, n, n, n, n, n, n, n, n,
               n, n, 5, 5, 5, n, 5, n, n, n, n, n, n, n, n, n, n,
               n, 5, 5, 5, 5, 5, 5, n, n, 5, 5, n, n, n, n, n, n,
               n, n, n, n, n, n, n, n, n, n, n, 5, 5, n, n, n, n),
               17,18
      )

      val RegionGroupResult(regions,regionMap) = run(RegionGroup(r))

      val histogram = run(GetHistogram(regions))
      val count = histogram.getValues.length
      count should be (8)
      
      val regionCounts = mutable.Map[Int,mutable.Set[Int]]()
      cfor(0)(_ < 17, _ + 1) { col =>
        cfor(0)(_ < 18, _ + 1) { row =>
          val v = r.get(col,row)
          val region = regions.get(col,row)

          if(v == NODATA) { region should be (v) }
          else {
            regionMap(region) should be (v)
            if(!regionCounts.contains(v)) { regionCounts(v) = mutable.Set[Int]() }
            regionCounts(v) += region
          }
        }
      }

      regionCounts(1).size should be (2)
      regionCounts(7).size should be (1)
      regionCounts(3).size should be (1)
      regionCounts(8).size should be (1)
      regionCounts(5).size should be (3)
    }

    it("should group regions when regions are concentric.") {
      val n = NODATA
      val r = createRaster(
        Array(
               n, n, 7, 7, 7, n, n,
               n, 7, 7, 5, 7, 7, n,
               7, 7, 5, 5, 5, 7, 7,
               7, 5, 5, 9, 5, 5, 7,
               7, 5, 5, 7, 5, 7, 7,
               7, 7, 5, 5, 5, 7, n,
               n, 7, 5, 5, 7, 7, n,
               n, 7, 7, 7, 7, n, n
        ),
               7,8
      )

      val RegionGroupResult(regions,regionMap) = run(RegionGroup(r))

      val histogram = run(GetHistogram(regions))
      val count = histogram.getValues.length
      count should be (4)
      
      val regionCounts = mutable.Map[Int,mutable.Set[Int]]()
      cfor(0)(_ < 7, _ + 1) { col =>
        cfor(0)(_ < 8, _ + 1) { row =>
          val v = r.get(col,row)
          val region = regions.get(col,row)
          if(v == NODATA) { region should be (v) }
          else {
            regionMap(region) should be (v)
            if(!regionCounts.contains(v)) { regionCounts(v) = mutable.Set[Int]() }
            regionCounts(v) += region
          }
        }
      }

      regionCounts(7).size should be (2)
      regionCounts(9).size should be (1)
      regionCounts(5).size should be (1)
    }

    it("should count regions with a nodata line almost separating regions") {
      val n = NODATA
      val arr = 
        Array(
//             0  1  2  3  4 
               1, 1, 1, 1, n,// 0
               1, 5, 5, 1, n,// 1
               5, 5, 1, 1, 1,// 2
               n, n, n, n, 1,// 3
               1, n, 1, n, 1,// 4
               1, 1, 1, 5, 1,// 5
               1, 5, 5, 1, 1,// 6
               1, 1, 1, 1, n)// 7

      val cw = 1
      val ch = 10
      val cols = 5
      val rows = 8
      val xmin = 0
      val xmax = 5
      val ymin = -70
      val ymax = 0

      val r = Raster(arr,RasterExtent(Extent(xmin,ymin,xmax,ymax),cw,ch,cols,rows))
      val RegionGroupResult(regions,regionMap) = run(RegionGroup(r))
      printR(regions)
      val histogram = run(GetHistogram(regions))
      val count = histogram.getValues.length
      count should be (4)
      
      val regionCounts = mutable.Map[Int,mutable.Set[Int]]()
      cfor(0)(_ < 5, _ + 1) { col =>
        cfor(0)(_ < 8, _ + 1) { row =>
          val v = r.get(col,row)
          val region = regions.get(col,row)
          if(v == NODATA) { region should be (v) }
          else {
            regionMap(region) should be (v)
            if(!regionCounts.contains(v)) { regionCounts(v) = mutable.Set[Int]() }
            regionCounts(v) += region
          }
        }
      }

    }
  }
}
