package geotrellis.spark.io.accumulo

import java.nio.ByteBuffer

import com.typesafe.scalalogging.slf4j.LazyLogging
import java.net.InetAddress
import org.apache.accumulo.core.client.impl.Tables
import org.apache.accumulo.core.client.mapreduce.lib.impl.{ConfiguratorBase => CB, InputConfigurator => IC}
import org.apache.accumulo.core.client.mapreduce.{InputFormatBase, AccumuloInputFormat}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.{TableOfflineException, TableDeletedException}
import org.apache.accumulo.core.data.{Range => ARange, Value, Key, KeyExtent}
import org.apache.accumulo.core.master.state.tables.TableState
import org.apache.accumulo.core.security.Credentials
import org.apache.accumulo.core.util.UtilWaitThread
import org.apache.hadoop.mapreduce.{RecordReader, TaskAttemptContext, InputSplit, JobContext}
import scala.collection.JavaConverters._

/** This input format will use Accumulo [TabletLocator] to create InputSplits for each tablet that contains
 * records from specified ranges. This is unlike AccumuloInputFormat which creates a single split per Range.
 * The MultiRangeInputSplits are intended to be read using a BatchScanner. This drastically reduces the number of
 * splits and consequnetly spark tasks that are produced by this InputFormat. Locality is preserved because tablets
 * may only be hosted by a single tablet server at a given time.
 *
 * Because RecordReader uses BatchScanner a number of modes are not supported: Offline, Isolated and Local Iterators.
 * These modes are backed by specalized scanners that only support scanning through a single range.
 *
 * We borrow some Accumulo machinery to set and read configurations so classOf AccumuloInputFormat should be used
 * for modifying Configuration, as if AccumuloInputFormat will be used.
 *
 * This class uses the internal Accumulo API and will likely not work across versions.
 *
 * WARNING: The locality of the splits rely on reverse resolution of tserver IPs matching those of spark workers.
 */
class BatchAccumuloInputFormat extends InputFormatBase[Key, Value] with LazyLogging {
  /** We're going to lie about our class so we can re-use Accumulo InputConfigurator to pull our Job settings */
  private val CLASS: Class[AccumuloInputFormat] =
    classOf[AccumuloInputFormat].asInstanceOf[Class[AccumuloInputFormat]]

  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val conf = context.getConfiguration
    require(IC.isOfflineScan(CLASS,conf) == false, "Offline scans not supported")
    require(IC.isIsolated(CLASS,conf) == false, "Isolated scans not supported")
    require(IC.usesLocalIterators(CLASS,conf) == false, "Local iterators not supported")

    val splits = new java.util.LinkedList[InputSplit]()
    val tableConfigs = IC.getInputTableConfigs(CLASS, conf)

    for ( tableConfigEntry <- tableConfigs.entrySet.asScala){
      val tableName = tableConfigEntry.getKey
      val tableConfig = tableConfigEntry.getValue
      val instance = CB.getInstance(CLASS, conf)
      var mockInstance: Boolean = false
      var tableId: String = ""

      if (instance.isInstanceOf[MockInstance]) {
        tableId = ""
        mockInstance = true
      } else {
        tableId = Tables.getTableId(instance, tableName)
        mockInstance = false
      }

      val auths = IC.getScanAuthorizations(CLASS, conf)
      val principal = CB.getPrincipal(CLASS, conf)
      val token = CB.getAuthenticationToken(CLASS, conf)
      val credentials = new Credentials(principal, token);
      var ranges = ARange.mergeOverlapping(tableConfig.getRanges())
      if (ranges.isEmpty()) {
        ranges = new java.util.ArrayList[ARange](1)
        ranges.add(new ARange())
      }

      /** Ranges binned by tablets */
      val binnedRanges = new java.util.HashMap[String, java.util.Map[KeyExtent, java.util.List[ARange]]]()
      val tabletLocator = IC.getTabletLocator(CLASS, conf, tableId)
      tabletLocator.invalidateCache

      // loop until list of tablet lookup failures is empty
      while (! tabletLocator.binRanges(credentials, ranges, binnedRanges).isEmpty ) {
        if (!(instance.isInstanceOf[MockInstance])) {
          if (!Tables.exists(instance, tableId))
            throw new TableDeletedException(tableId)
          if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
            throw new TableOfflineException(instance, tableId)
        }
        binnedRanges.clear()
        logger.warn("Unable to locate bins for specified ranges. Retrying.")
        Thread.sleep(100 + scala.util.Random.nextInt(100)); // sleep randomly between 100 and 200 ms
        tabletLocator.invalidateCache()
      }

      // tserver: String = server:ip for the tablet server
      // tserverBin: Map[KeyExtent, List[ARange]]
      binnedRanges.asScala map { case (tserver, tserverBin) =>
        tserverBin.asScala.map { case (keyExtent, extentRanges) =>
          val ip = tserver.split(":").head
          val tabletRange = keyExtent.toDataRange
          val split = new MultiRangeInputSplit()
          val exr = extentRanges.asScala
          split.ranges =
            if (exr.isEmpty)
              List(new ARange())
            else
              exr map { tabletRange.clip }
          split.iterators = IC.getIterators(CLASS, conf).asScala.toList
          split.location = InetAddress.getByName(ip).getCanonicalHostName()
          split.table = tableName
          split.instanceName = instance.getInstanceName
          split.zooKeepers = instance.getZooKeepers
          split.principal = principal
          split.token = token
          split.fetchedColumns = IC.getFetchedColumns(CLASS, conf).asScala
          instance match {
            case _: MockInstance      => split.mockInstance = true
            case _: ZooKeeperInstance => split.mockInstance = false
            case _ => sys.error("Unknown instance type")
          }

          splits.add(split)
        }
      }
    }
    splits
  }

  override def createRecordReader(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): RecordReader[Key, Value] = {
    val reader = new MultiRangeRecordReader()
    reader.initialize(inputSplit, taskAttemptContext)
    reader
  }
}
