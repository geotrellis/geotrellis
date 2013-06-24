package geotrellis.raster.op.local

import geotrellis._

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

import geotrellis.testutil._

class VarietySpec extends FunSpec 
                     with ShouldMatchers 
                     with TestServer 
                     with RasterBuilders {
  describe("Variety") {
    it("computes variety") { 
      val n = NODATA
      val r1 = createRaster(
        Array( n, n, n, n, n,
               n, n, n, n, n,
               n, n, n, n, n,
               n, n, n, n, n), 
        5,4
      )

      val r2 = createRaster(
        Array( n, 1, n, n, n,
               n, n, 1, n, n,
               n, n, n, 1, n,
               n, n, n, n, 1), 
        5,4
      )

      val r3 = createRaster(
        Array( n, 2, n, n, n,
               n, n, 2, n, n,
               n, n, n, 2, n,
               n, n, n, n, 1), 
        5,4
      )

      val r4 = createRaster(
        Array( n, 3, n, n, n,
               n, n, 3, n, n,
               n, n, n, 2, n,
               n, n, n, n, 1), 
        5,4
      )

      val r5 = createRaster(
        Array( n, 4, n, n, n,
               n, n, 3, n, n,
               n, n, n, 2, n,
               n, n, n, n, 1), 
        5,4
      )

      val variety = run(Variety(r1,r2,r3,r4,r5))
      for(col <- 0 until 5) {
        for(row <- 0 until 4) {
          if(col== row + 1) {
            col match {
              case 1 => variety.get(col,row) should be (4)
              case 2 => variety.get(col,row) should be (3)
              case 3 => variety.get(col,row) should be (2)
              case 4 => variety.get(col,row) should be (1)
            }
          } else {
            variety.get(col,row) should be (NODATA)
          }
        }
      }
    }
  }
}
