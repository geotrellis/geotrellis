package geotrellis.spark.io.hadoop

import com.github.nscala_time.time.Imports._
import geotrellis.raster.Tile
import geotrellis.spark.io._
import geotrellis.spark.io.json._
import geotrellis.spark.io.index._
import geotrellis.spark.testfiles.TestFiles
import geotrellis.spark._
import geotrellis.spark.io.avro.codecs._
import org.joda.time.DateTime

abstract class HadoopSpaceTimeSpec
  extends PersistenceSpec[SpaceTimeKey, Tile, RasterMetadata]
    with TestEnvironment
    with TestFiles
    with CoordinateSpaceTimeTests {
  lazy val reindexerKeyIndexMethod = ZCurveKeyIndexMethod.byMonth

  lazy val reader    = HadoopLayerReader[SpaceTimeKey, Tile, RasterMetadata](outputLocal)
  lazy val deleter   = HadoopLayerDeleter(outputLocal)
  lazy val copier    = HadoopLayerCopier[SpaceTimeKey, Tile, RasterMetadata](outputLocal)
  lazy val mover     = HadoopLayerMover[SpaceTimeKey, Tile, RasterMetadata](outputLocal)
  lazy val reindexer = HadoopLayerReindexer[SpaceTimeKey, Tile, RasterMetadata](outputLocal)
  lazy val tiles     = HadoopTileReader[SpaceTimeKey, Tile](outputLocal)
  lazy val writer    = HadoopLayerWriter[SpaceTimeKey, Tile, RasterMetadata](outputLocal)
  lazy val sample    = CoordinateSpaceTime
}

class HadoopSpaceTimeZCurveByYearSpec extends HadoopSpaceTimeSpec {
  lazy val writerKeyIndexMethod = ZCurveKeyIndexMethod.byYear
}

/*class HadoopSpaceTimeZCurveByFuncSpec extends HadoopSpaceTimeSpec {
  lazy val writerKeyIndexMethod = ZCurveKeyIndexMethod.by({ x =>  if (x < DateTime.now) 1 else 0 }, "HadoopSpaceTimeZCurveByFuncSpec")
}*/

class HadoopSpaceTimeHilbertSpec extends HadoopSpaceTimeSpec {
  lazy val writerKeyIndexMethod = HilbertKeyIndexMethod(DateTime.now - 20.years, DateTime.now, 4)
}

class HadoopSpaceTimeHilbertWithResolutionSpec extends HadoopSpaceTimeSpec {
  lazy val writerKeyIndexMethod = HilbertKeyIndexMethod(2)
}
