package geotrellis.spark.io.accumulo

import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.mapreduce.{AbstractInputFormat => AIF, AccumuloOutputFormat => AOF}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job

import scala.collection.JavaConversions._

trait AccumuloInstance  extends Serializable {
  def connector: Connector
  def instanceName: String
  def setAccumuloConfig(job: Job): Unit

  def ensureTableExists(tableName: String): Unit = {
    val ops = connector.tableOperations()
    if (!ops.exists(tableName))
      ops.create(tableName)
  }

  def makeLocalityGroup(tableName: String, columnFamily: String): Unit = {
    val ops = connector.tableOperations()
    val groups = ops.getLocalityGroups(tableName)
    val newGroup: java.util.Set[Text] = Set(new Text(columnFamily))
    ops.setLocalityGroups(tableName, groups.updated(tableName, newGroup))
  }
}

object AccumuloInstance {
  def apply(instanceName: String, zookeeper: String, user: String, token: AuthenticationToken): AccumuloInstance = {
    val tokenBytes = AuthenticationToken.AuthenticationTokenSerializer.serialize(token)
    val tokenClass = token.getClass.getCanonicalName
    BaseAccumuloInstance(instanceName, zookeeper, user, (tokenClass, tokenBytes))
  }
}

case class BaseAccumuloInstance(
  instanceName: String, zookeeper: String,
  user: String, tokenBytes: (String, Array[Byte])) extends AccumuloInstance
{
  @transient lazy val token = AuthenticationToken.AuthenticationTokenSerializer.deserialize(tokenBytes._1, tokenBytes._2)
  @transient lazy val instance: Instance = instanceName match {
    case "fake" => new MockInstance("fake") //in-memory only
    case _      => new ZooKeeperInstance(instanceName, zookeeper)
  }
  @transient lazy val connector: Connector = instance.getConnector(user, token)


  def setAccumuloConfig(job: Job): Unit = {
    val clientConfig = ClientConfiguration
      .loadDefault()
      .withZkHosts(zookeeper)
      .withInstance(instanceName)


    if (instanceName == "fake") {
      AIF.setMockInstance(job, instanceName)
      AOF.setMockInstance(job, instanceName)
    }
    else {
      AIF.setZooKeeperInstance(job, clientConfig)
      AOF.setZooKeeperInstance(job, clientConfig)
    }

    AIF.setConnectorInfo(job, user, token)
    AOF.setConnectorInfo(job, user, token)
  }
}
