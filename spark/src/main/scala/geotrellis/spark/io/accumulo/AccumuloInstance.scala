package geotrellis.spark.io.accumulo

import geotrellis.spark._
import geotrellis.spark.io.DefaultParams
import geotrellis.spark.tiling._
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.mapreduce.{InputFormatBase, AccumuloInputFormat, AccumuloOutputFormat}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken
import org.apache.accumulo.core.client.mapreduce.lib.util.{ConfiguratorBase => CB}
import org.apache.accumulo.core.data.{Value, Key, Mutation}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import com.typesafe.config.{ConfigFactory,Config}

case class AccumuloInstance(
  instanceName: String, zookeeper: String,
  user: String, token: AuthenticationToken)
{
  val instance: Instance = instanceName match {
    case "fake" => new MockInstance("fake") //in-memory only
    case _      => new ZooKeeperInstance(instanceName, zookeeper)
  }
  val connector = instance.getConnector(user, token)

  /** The value is specified in reference.conf, applications can overwrite it in their application.conf */
  val catalogTable: String = {
    ConfigFactory.load().getString("geotrellis.accumulo.catalog")
  }

  val attributeCatalog = new AccumuloAttributeCatalog(connector, catalogTable)

  def catalog(config: DefaultParams[String])(implicit sc: SparkContext) =
    AccumuloCatalog(sc, this, attributeCatalog, config)

  def catalog(implicit sc: SparkContext) =
    AccumuloCatalog(sc, this, attributeCatalog, AccumuloCatalog.BaseParamsConfig)

  def setAccumuloConfig(conf: Configuration): Unit = {
    if (instanceName == "fake") {
      CB.setMockInstance(classOf[AccumuloInputFormat], conf, instanceName)
      CB.setMockInstance(classOf[AccumuloOutputFormat], conf, instanceName)
    }
    else {
      CB.setZooKeeperInstance(classOf[AccumuloInputFormat],conf, instanceName, zookeeper)
      CB.setZooKeeperInstance(classOf[AccumuloOutputFormat],conf, instanceName, zookeeper)
    }

    CB.setConnectorInfo(classOf[AccumuloInputFormat], conf, user, token)
    CB.setConnectorInfo(classOf[AccumuloOutputFormat], conf, user, token)
  }

  def setAccumuloConfig(job: Job): Unit = setAccumuloConfig(job.getConfiguration)

  def setAccumuloConfig(sc: SparkContext): Unit = setAccumuloConfig(sc.hadoopConfiguration)
}
