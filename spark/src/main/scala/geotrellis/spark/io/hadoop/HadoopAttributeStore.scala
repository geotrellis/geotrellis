package geotrellis.spark.io.hadoop

import geotrellis.spark._
import geotrellis.spark.io._

import spray.json._
import DefaultJsonProtocol._
import org.apache.hadoop.fs.Path
import org.apache.spark._
import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration

class HadoopAttributeStore(val rootPath: Path, val hadoopConfiguration: Configuration) extends AttributeStore[JsonFormat] {
  val attributePath = new Path(rootPath, "_attributes")
  val fs = attributePath.getFileSystem(hadoopConfiguration)

  val SEP = "___"

  // Create directory if it doesn't exist
  if(!fs.exists(attributePath)) {
    fs.mkdirs(attributePath)
  }

  def attributePath(layerId: LayerId, attributeName: String): Path = {
    val fname = s"${layerId.name}${SEP}${layerId.zoom}${SEP}${attributeName}.json"
    new Path(attributePath, fname)
  }

  private def delete(layerId: LayerId, path: Path): Unit = {
    if(!layerExists(layerId)) throw new LayerNotFoundError(layerId)
    HdfsUtils
      .listFiles(new Path(attributePath, path), hadoopConfiguration)
      .foreach(fs.delete(_, false))
  }

  def attributeWildcard(attributeName: String): Path =
    new Path(s"*${SEP}${attributeName}.json")

  private def readFile[T: Format](path: Path): Option[(LayerId, T)] = {
    HdfsUtils
      .getLineScanner(path, hadoopConfiguration)
      .map{ in =>
        val txt =
          try {
            in.mkString
          }
          finally {
            in.close()
          }
        txt.parseJson.convertTo[(LayerId, T)]
      }
  }

  def read[T: Format](layerId: LayerId, attributeName: String): T =
    readFile[T](attributePath(layerId, attributeName)) match {
      case Some((id, value)) => value
      case None => throw new AttributeNotFoundError(attributeName, layerId)
    }

  def readAll[T: Format](attributeName: String): Map[LayerId,T] = {
    HdfsUtils
      .listFiles(attributeWildcard(attributeName), hadoopConfiguration)
      .map{ path: Path =>
        readFile[T](path) match {
          case Some(tup) => tup
          case None => throw new LayerIOError(s"Unable to list $attributeName attributes from $path")
        }
      }
      .toMap
  }

  def write[T: Format](layerId: LayerId, attributeName: String, value: T): Unit = {
    val path = attributePath(layerId, attributeName)

    if(fs.exists(path)) {
      fs.delete(path, false)
    }

    val fdos = fs.create(path)
    val out = new PrintWriter(fdos)
    try {
      val s = (layerId, value).toJson.toString()
      out.println(s)
    } finally {
      out.close()
      fdos.close()
    }
  }

  def layerExists(layerId: LayerId): Boolean =
    HdfsUtils
      .listFiles(new Path(attributePath, s"*.json"), hadoopConfiguration)
      .exists { path: Path =>
        val List(name, zoomStr) = path.getName.split(SEP).take(2).toList
        layerId == LayerId(name, zoomStr.toInt)
      }

  def delete(layerId: LayerId): Unit =
    delete(layerId, new Path(s"${layerId.name}${SEP}${layerId.zoom}${SEP}*.json"))

  def delete(layerId: LayerId, attributeName: String): Unit =
    delete(layerId, new Path(s"${layerId.name}${SEP}${layerId.zoom}${SEP}${attributeName}.json"))

  def layerIds: Seq[LayerId] =
    HdfsUtils
      .listFiles(new Path(attributePath, s"*.json"), hadoopConfiguration)
      .map { path: Path =>
        val List(name, zoomStr) = path.getName.split(SEP).take(2).toList
        LayerId(name, zoomStr.toInt)
      }
      .distinct
}

object HadoopAttributeStore {
  def apply(rootPath: Path, config: Configuration): HadoopAttributeStore =
    new HadoopAttributeStore(rootPath, config)

  def apply(rootPath: Path)(implicit sc: SparkContext): HadoopAttributeStore =
    apply(rootPath, sc.hadoopConfiguration)
}
