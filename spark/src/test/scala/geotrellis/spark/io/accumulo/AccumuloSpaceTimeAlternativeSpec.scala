package geotrellis.spark.io.accumulo

import geotrellis.raster.Tile
import geotrellis.spark.io.accumulo.spacetime.{SpaceTimeAccumuloRDDReader, SpaceTimeAccumuloRDDWriter}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.testfiles.TestFiles
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.json._
import geotrellis.spark.io.avro.codecs._

class AccumuloSpaceTimeAlternativeSpec 
    extends PersistenceSpec[SpaceTimeKey, Tile, RasterMetadata]
    with TestEnvironment
    with TestFiles
    with CoordinateSpaceTimeTests
    with LayerUpdateSpaceTimeTileTests {
  override val layerId  = LayerId(name, 1)
  implicit val instance = MockAccumuloInstance()

  lazy val writerKeyIndexMethod    = ZCurveKeyIndexMethod.byYear
  lazy val reindexerKeyIndexMethod = ZCurveKeyIndexMethod.byMonth

  lazy val reader = new AccumuloLayerReader[SpaceTimeKey, Tile, RasterMetadata](
    AccumuloAttributeStore(instance.connector),
    new SpaceTimeAccumuloRDDReader[Tile](instance))

  lazy val writer =
    new AccumuloLayerWriter[SpaceTimeKey, Tile, RasterMetadata](
      attributeStore = AccumuloAttributeStore(instance.connector),
      rddWriter = new SpaceTimeAccumuloRDDWriter[Tile](instance, SocketWriteStrategy()),
      table = "tiles")

  lazy val updater = new AccumuloLayerUpdater[SpaceTimeKey, Tile, RasterMetadata](
    AccumuloAttributeStore(instance.connector),
    new SpaceTimeAccumuloRDDWriter[Tile](instance, SocketWriteStrategy()))

  lazy val deleter   = new AccumuloLayerDeleter(AccumuloAttributeStore(instance.connector), instance.connector)
  lazy val copier    = AccumuloLayerCopier[SpaceTimeKey, Tile, RasterMetadata](instance, reader, writer)
  lazy val mover     = GenericLayerMover(copier, deleter)
  lazy val reindexer = AccumuloLayerReindexer[SpaceTimeKey, Tile, RasterMetadata](instance, "tiles", SocketWriteStrategy())

  lazy val tiles  = AccumuloTileReader[SpaceTimeKey, Tile](instance)
  lazy val sample = CoordinateSpaceTime
}
