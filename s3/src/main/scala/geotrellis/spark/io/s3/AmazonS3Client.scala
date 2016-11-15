package geotrellis.spark.io.s3

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth._
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.services.s3.{AmazonS3Client => AWSAmazonS3Client}
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.s3.model._
import org.apache.commons.io.IOUtils
import com.typesafe.scalalogging.LazyLogging

import java.io.{InputStream, ByteArrayInputStream}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

object AmazonS3Client {
  def apply(s3client: AWSAmazonS3Client): AmazonS3Client =
    new AmazonS3Client(s3client)

  def apply(credentials: AWSCredentials, config: ClientConfiguration): AmazonS3Client =
    apply(new AWSAmazonS3Client(credentials, config))

  def apply(provider: AWSCredentialsProvider, config: ClientConfiguration): AmazonS3Client =
    apply(new AWSAmazonS3Client(provider, config))

  def apply(provider: AWSCredentialsProvider): AmazonS3Client =
    apply(provider, new ClientConfiguration())

}

class AmazonS3Client(s3client: AWSAmazonS3Client) extends S3Client {
  def listObjects(listObjectsRequest: ListObjectsRequest): ObjectListing =
    s3client.listObjects(listObjectsRequest)

  def listKeys(listObjectsRequest: ListObjectsRequest): Seq[String] = {
    var listing: ObjectListing = null
    val result = mutable.ListBuffer[String]()
    do {
      listing = s3client.listObjects(listObjectsRequest)
      // avoid including "directories" in the input split, can cause 403 errors on GET
      result ++= listing.getObjectSummaries.asScala.map(_.getKey).filterNot(_ endsWith "/")
      listObjectsRequest.setMarker(listing.getNextMarker)
    } while (listing.isTruncated)

    result.toSeq
  }

  def getObject(getObjectRequest: GetObjectRequest): S3Object =
    s3client.getObject(getObjectRequest)

  def putObject(putObjectRequest: PutObjectRequest): PutObjectResult =
    s3client.putObject(putObjectRequest)

  def deleteObject(deleteObjectRequest: DeleteObjectRequest): Unit =
    s3client.deleteObject(deleteObjectRequest)

  def copyObject(copyObjectRequest: CopyObjectRequest): CopyObjectResult =
    s3client.copyObject(copyObjectRequest)

  def listNextBatchOfObjects(listing: ObjectListing): ObjectListing =
    s3client.listNextBatchOfObjects(listing)

  def deleteObjects(deleteObjectsRequest: DeleteObjectsRequest): Unit =
    s3client.deleteObjects(deleteObjectsRequest)

  def readBytes(getObjectRequest: GetObjectRequest): Array[Byte] = {
    val obj = s3client.getObject(getObjectRequest)
    val inStream = obj.getObjectContent
    try {
      IOUtils.toByteArray(inStream)
    } finally {
      inStream.close()
    }
  }

  def readRange(start: Long, end: Long, getObjectRequest: GetObjectRequest): Array[Byte] = {
    getObjectRequest.setRange(start, end - 1)
    val obj = s3client.getObject(getObjectRequest)
    val stream = obj.getObjectContent
    try {
      IOUtils.toByteArray(stream)
    } finally {
      stream.close()
    }
  }

  def getObjectMetadata(getObjectMetadataRequest: GetObjectMetadataRequest): ObjectMetadata =
    s3client.getObjectMetadata(getObjectMetadataRequest)

  def setRegion(region: com.amazonaws.regions.Region): Unit = {
    s3client.setRegion(region)
  }
}
