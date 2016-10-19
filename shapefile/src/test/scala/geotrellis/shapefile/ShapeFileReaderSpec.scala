package geotrellis.shapefile

import geotrellis.vector._

import org.scalatest._

class ShapeFileReaderSpec extends FunSpec with Matchers {
  describe("ShapeFileReader") {
    it("should read multipolygons feature attribute") {
      val path = "shapefile/data/shapefiles/demographics/demographics.shp"
      val features = ShapeFileReader.readMultiPolygonFeatures(path)
      features.size should be (160)
      for(MultiPolygonFeature(polygon: MultiPolygon, data: Map[String, Object]) <- features) {
        data.keys.toSeq should be (Seq("LowIncome", "gbcode", "ename", "WorkingAge", "TotalPop", "Employment"))
      }
    }
  }
}
