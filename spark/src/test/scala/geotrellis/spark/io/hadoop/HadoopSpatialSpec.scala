package geotrellis.spark.io.hadoop

import geotrellis.raster.Tile
import geotrellis.spark.io._
import geotrellis.spark.io.json._
import geotrellis.spark.io.index._
import geotrellis.spark.testfiles.TestFiles
import geotrellis.spark._
import geotrellis.spark.io.avro.codecs._

abstract class HadoopSpatialSpec
  extends PersistenceSpec[SpatialKey, Tile, RasterMetaData]
    with TestEnvironment
    with TestFiles
    with AllOnesTestTileTests {
  lazy val reindexerKeyIndexMethod: KeyIndexMethod[SpatialKey] = ZCurveKeyIndexMethod

  lazy val reader    = HadoopLayerReader[SpatialKey, Tile, RasterMetaData](outputLocal)
  lazy val deleter   = HadoopLayerDeleter(outputLocal)
  lazy val copier    = HadoopLayerCopier[SpatialKey, Tile, RasterMetaData](outputLocal)
  lazy val mover     = HadoopLayerMover[SpatialKey, Tile, RasterMetaData](outputLocal)
  lazy val reindexer = HadoopLayerReindexer[SpatialKey, Tile, RasterMetaData](outputLocal)
  lazy val tiles     = HadoopTileReader[SpatialKey, Tile](outputLocal)
  lazy val writer    = HadoopLayerWriter[SpatialKey, Tile, RasterMetaData](outputLocal)
  lazy val sample    = AllOnesTestFile
}

class HadoopSpatialRowMajorSpec extends HadoopSpatialSpec {
  lazy val writerKeyIndexMethod: KeyIndexMethod[SpatialKey] = RowMajorKeyIndexMethod
}

class HadoopSpatialZCurveSpec extends HadoopSpatialSpec {
  lazy val writerKeyIndexMethod: KeyIndexMethod[SpatialKey] = ZCurveKeyIndexMethod
}

class HadoopSpatialHilbertSpec extends HadoopSpatialSpec {
  lazy val writerKeyIndexMethod: KeyIndexMethod[SpatialKey] = HilbertKeyIndexMethod
}


