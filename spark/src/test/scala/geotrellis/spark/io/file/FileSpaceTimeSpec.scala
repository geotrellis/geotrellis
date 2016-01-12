package geotrellis.spark.io.file

import geotrellis.raster.Tile
import geotrellis.spark.io._
import geotrellis.spark.io.json._
import geotrellis.spark.io.index._
import geotrellis.spark.testfiles.TestFiles
import geotrellis.spark._
import geotrellis.spark.io.avro.codecs._
import com.github.nscala_time.time.Imports._

abstract class FileSpaceTimeSpec
    extends PersistenceSpec[SpaceTimeKey, Tile, RasterMetaData]
    with TestEnvironment with TestFiles
    with CoordinateSpaceTimeTests {
  val catalogPath = outputLocalPath

  lazy val reader = FileLayerReader[SpaceTimeKey, Tile, RasterMetaData](catalogPath)
  lazy val deleter = FileLayerDeleter[SpaceTimeKey, Tile, RasterMetaData](catalogPath)
  lazy val copier = FileLayerCopier[SpaceTimeKey, Tile, RasterMetaData](catalogPath)
  lazy val mover  = FileLayerMover[SpaceTimeKey, Tile, RasterMetaData](catalogPath)
  lazy val reindexer = FileLayerReindexer[SpaceTimeKey, Tile, RasterMetaData](catalogPath, ZCurveKeyIndexMethod.byPattern("YMM"))
  lazy val tiles = FileTileReader[SpaceTimeKey, Tile](catalogPath)
  lazy val sample =  CoordinateSpaceTime
}

class FileSpaceTimeZCurveByYearSpec extends FileSpaceTimeSpec {
  lazy val writer = FileLayerWriter[SpaceTimeKey, Tile, RasterMetaData](catalogPath, ZCurveKeyIndexMethod.byYear)
}

/*class FileSpaceTimeZCurveByFuncSpec extends FileSpaceTimeSpec {
  lazy val writer = FileLayerWriter[SpaceTimeKey, Tile, RasterMetaData](catalogPath, ZCurveKeyIndexMethod.by{ x =>  if (x < DateTime.now) 1 else 0 })
}*/

class FileSpaceTimeHilbertSpec extends FileSpaceTimeSpec {
  lazy val writer = FileLayerWriter[SpaceTimeKey, Tile, RasterMetaData](catalogPath, HilbertKeyIndexMethod(DateTime.now - 20.years, DateTime.now, 4))
}

class FileSpaceTimeHilbertWithResolutionSpec extends FileSpaceTimeSpec {
  lazy val writer = FileLayerWriter[SpaceTimeKey, Tile, RasterMetaData](catalogPath,  HilbertKeyIndexMethod(2))
}
