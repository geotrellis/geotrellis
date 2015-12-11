package geotrellis.spark.io.s3

import geotrellis.spark.LayerId
import geotrellis.spark.io._
import spray.json.JsonFormat
import spray.json.DefaultJsonProtocol._

class S3LayerDeleter(attributeStore: AttributeStore[JsonFormat]) extends LayerDeleter[LayerId] {

  def getS3Client: () => S3Client = () => S3Client.default

  def delete(id: LayerId): Unit = {
    val (header, _, _, _, _) = try {
      attributeStore.readLayerAttributes[S3LayerHeader, Unit, Unit, Unit, Unit](id)
    } catch {
      case e: AttributeNotFoundError => throw new LayerNotFoundError(id).initCause(e)
    }

    val bucket = header.bucket
    val prefix = header.key
    val s3Client = getS3Client()

    s3Client.deleteListing(bucket, s3Client.listObjects(bucket, prefix))
    attributeStore.delete(id)
    attributeStore.clearCache()
  }
}

object S3LayerDeleter {
  def apply(bucket: String, prefix: String) = new S3LayerDeleter(S3AttributeStore(bucket, prefix))
}
