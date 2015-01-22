package geotrellis.spark.io.accumulo

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.utils.TimedCache

import spray.json._

import scala.collection.JavaConversions._

import org.apache.spark.Logging
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.data._
import org.apache.hadoop.io.Text

class AccumuloAttributeCatalog(connector: Connector, val attributeTable: String) extends AttributeCatalog with Logging {
  type ReadableWritable[T] = JsonFormat[T]
  
  val cache = new TimedCache[(LayerId, String), Any](1000*30)

  {
    val ops = connector.tableOperations()
    if (!ops.exists(attributeTable))
      ops.create(attributeTable)
  }

  def getScanner = connector.createScanner(attributeTable, new Authorizations())
  
  private def fetch(layerId: LayerId, attributeName: String): List[Value] = {
    val scanner  = getScanner
    scanner.setRange(new Range(new Text(layerId.toString)))
    scanner.fetchColumnFamily(new Text(attributeName))
    scanner.iterator.toList.map(_.getValue)
  }

  def load[T: JsonFormat](layerId: LayerId, attributeName: String): T = {
    cache get (layerId -> attributeName) match {
      case Some(value) =>    
        value.asInstanceOf[T]
      case None =>
        val values = fetch(layerId, attributeName)

        if(values.size == 0) {
          throw new LayerNotFoundError(layerId)
        } else if(values.size > 1) {
          // should not be possible, but for completeness 
          throw new MultipleMatchError(layerId)
        } else {
          val value = values.head.toString.parseJson.convertTo[T]
          cache put (layerId -> attributeName, value)
          value
        }
    }
  }

  def save[T: JsonFormat](layerId: LayerId, attributeName: String, value: T): Unit = {
    val mutation = new Mutation(layerId.toString)  
    
    mutation.put(
      new Text(attributeName), new Text(), System.currentTimeMillis(),
      new Value(value.toJson.compactPrint.getBytes)
    )

    connector.write(attributeTable, mutation)
    cache put (layerId -> attributeName, value)
  }

  def listLayers: List[LayerId] = {
    val scanner = getScanner
    scanner.fetchColumnFamily(AttributeCatalog.KEYCLASS_FIELD)
    scanner.iterator.toList.map{ row => 
      LayerId.fromString(row.getKey.toString)
    }
  }
}