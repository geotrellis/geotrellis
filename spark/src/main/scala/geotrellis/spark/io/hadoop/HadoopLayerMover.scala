package geotrellis.spark.io.hadoop

import geotrellis.spark.io.index.{KeyIndexMethod, KeyIndex}
import geotrellis.spark.{KeyBounds, LayerId}
import geotrellis.spark.io._
import geotrellis.spark.io.json._

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

import spray.json.JsonFormat
import spray.json.DefaultJsonProtocol._

import scala.reflect.ClassTag

class HadoopLayerMover[K: JsonFormat: ClassTag, V: ClassTag, M: JsonFormat]
  (rootPath: Path, val attributeStore: AttributeStore[JsonFormat])
  (implicit sc: SparkContext) extends LayerMover[LayerId, K] {

  def move[I <: KeyIndex[K]: JsonFormat](from: LayerId, to: LayerId): Unit = {
    if (!attributeStore.layerExists(from)) throw new LayerNotFoundError(from)
    if (attributeStore.layerExists(to)) throw new LayerExistsError(to)

    val (header, metadata, keyBounds, keyIndex, _) = try {
      attributeStore.readLayerAttributes[HadoopLayerHeader, M, KeyBounds[K], I, Unit](from)
    } catch {
      case e: AttributeNotFoundError => throw new LayerMoveError(from, to).initCause(e)
    }
    val newPath = new Path(rootPath,  s"${to.name}/${to.zoom}")
    HdfsUtils.renamePath(header.path, newPath, sc.hadoopConfiguration)
    attributeStore.writeLayerAttributes(
      to, header.copy(path = newPath), metadata, keyBounds, keyIndex, Option.empty[Schema]
    )
    attributeStore.delete(from)
    attributeStore.clearCache()
  }

  // not supported operations
  def move[I <: KeyIndex[K]: JsonFormat](from: LayerId, to: LayerId, keyIndex: I): Unit =
    move[I](from, to)

  def move(from: LayerId, to: LayerId, keyIndexMethod: KeyIndexMethod[K]): Unit =
    move[KeyIndex[K]](from, to)
}

object HadoopLayerMover {
  def apply[K: JsonFormat: ClassTag, V: ClassTag, M: JsonFormat]
    (rootPath: Path, attributeStore: AttributeStore[JsonFormat])
    (implicit sc: SparkContext): HadoopLayerMover[K, V, M] =
      new HadoopLayerMover[K, V, M](rootPath, attributeStore)

  def apply[K: JsonFormat: ClassTag, V: ClassTag, M: JsonFormat]
    (rootPath: Path)(implicit sc: SparkContext): HadoopLayerMover[K, V, M] =
    apply[K, V, M](rootPath, HadoopAttributeStore(new Path(rootPath, "attributes"), new Configuration))
}
