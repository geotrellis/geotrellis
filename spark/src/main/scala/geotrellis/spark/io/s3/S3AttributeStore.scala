package geotrellis.spark.io.s3

import java.nio.charset.Charset

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.json._

import spray.json._
import DefaultJsonProtocol._

import org.apache.spark._
import java.io.PrintWriter
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.model.{ObjectMetadata, AmazonS3Exception}
import scala.io.Source
import java.io.ByteArrayInputStream

/**
 * Stores and retrieves layer attributes in an S3 bucket in JSON format
 * 
 * @param bucket    S3 bucket to use for attribute store
 * @param rootPath  path in the bucket for given LayerId, not ending in "/"
 */
class S3AttributeStore(bucket: String, rootPath: String) extends AttributeStore[JsonFormat] {
  val s3Client: S3Client = S3Client.default

  /** NOTE:
   * S3 is eventually consistent, therefore it is possible to write an attribute and fail to read it
   * immediately afterwards. It is not clear if this is a practical concern.
   * It could be remedied by some kind of time-out cache for both read/write in this class.
   */

  def path(parts: String*) = parts.filter(_.nonEmpty).mkString("/")

  def attributePath(id: LayerId, attributeName: String): String =
    path(rootPath, "_attributes", s"${attributeName}__${id.name}__${id.zoom}.json")

  def attributePrefix(attributeName: String): String =
    path(rootPath, "_attributes", s"${attributeName}__")

  private def readKey[T: Format](key: String): (LayerId, T) = {
    val is = s3Client.getObject(bucket, key).getObjectContent
    val json =
      try {
        Source.fromInputStream(is)(Charset.forName("UTF-8")).mkString
      } finally {
        is.close()
      }

    json.parseJson.convertTo[(LayerId, T)]
  }
  
  def read[T: Format](layerId: LayerId, attributeName: String): T =
    try {
      readKey[T](attributePath(layerId, attributeName))._2
    } catch {
      case e: AmazonS3Exception =>
        throw new AttributeNotFoundError(attributeName, layerId).initCause(e)
    }

  def readAll[T: Format](attributeName: String): Map[LayerId, T] =
    s3Client
      .listObjectsIterator(bucket, attributePrefix(attributeName))
      .map{ os =>
        try {
          readKey[T](os.getKey)
        } catch {
          case e: AmazonS3Exception =>
            throw new CatalogError(s"Unable to list $attributeName attributes from $bucket/${os.getKey}").initCause(e)
        }
      }
      .toMap

  def write[T: Format](layerId: LayerId, attributeName: String, value: T): Unit = {
    val key = attributePath(layerId, attributeName)
    val str = (layerId, value).toJson.compactPrint
    val is = new ByteArrayInputStream(str.getBytes("UTF-8"))
    s3Client.putObject(bucket, key, is, new ObjectMetadata())
    //AmazonServiceException possible
  }

  def layerExists(layerId: LayerId): Boolean =
    s3Client.listObjectsIterator(bucket, attributePath(layerId, AttributeStore.Fields.metaData), 1).nonEmpty

  def delete(layerId: LayerId, path: String): Unit = {
    if(!layerExists(layerId)) throw new LayerNotFoundError(layerId)
    s3Client.deleteObject(bucket, path)
  }

  def delete(layerId: LayerId): Unit = {
    if(!layerExists(layerId)) throw new LayerNotFoundError(layerId)
    s3Client
      .listObjectsIterator(bucket, path(rootPath, "_attributes"))
      .foreach { os =>
        if(os.getKey.contains(s"__${layerId.name}__${layerId.zoom}.json")) {
          s3Client.deleteObject(bucket, os.getKey)
        }
      }
  }

}

object S3AttributeStore {
  def apply(bucket: String, root: String) =
    new S3AttributeStore(bucket, root)

  def apply(bucket: String): S3AttributeStore =
    apply(bucket, "")
}
