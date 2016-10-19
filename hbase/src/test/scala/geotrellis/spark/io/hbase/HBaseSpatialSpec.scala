package geotrellis.spark.io.hbase

import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.testfiles.TestFiles

class HBaseSpatialSpec
  extends PersistenceSpec[SpatialKey, Tile, TileLayerMetadata[SpatialKey]]
    with SpatialKeyIndexMethods
    with HBaseTestEnvironment
    with TestFiles
    with AllOnesTestTileSpec {

  registerAfterAll { () =>
    HBaseInstance(Seq("localhost"), "localhost").withAdminDo { admin =>
      admin.disableTable("metadata")
      admin.disableTable("tiles")
      admin.deleteTable("metadata")
      admin.deleteTable("tiles")
    }
  }

  lazy val instance       = HBaseInstance(Seq("localhost"), "localhost")
  lazy val attributeStore = HBaseAttributeStore(instance)

  lazy val reader    = HBaseLayerReader(attributeStore)
  lazy val creader   = HBaseLayerCollectionReader(attributeStore)
  lazy val writer    = HBaseLayerWriter(attributeStore, "tiles")
  lazy val deleter   = HBaseLayerDeleter(attributeStore)
  lazy val updater   = HBaseLayerUpdater(attributeStore)
  lazy val tiles     = HBaseValueReader(attributeStore)
  lazy val sample    = AllOnesTestFile
  lazy val copier    = HBaseLayerCopier(attributeStore, reader, writer)
  lazy val reindexer = HBaseLayerReindexer(attributeStore, reader, writer, deleter, copier)
  lazy val mover     = HBaseLayerMover(copier, deleter)
}
