package org.corespring.amazon.s3

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{GetObjectMetadataRequest, ObjectMetadata, S3Object}
import com.amazonaws.{AmazonServiceException, AmazonClientException}
import java.io.{PipedInputStream, PipedOutputStream}
import org.corespring.amazon.s3.models._
import play.api.libs.iteratee._
import play.api.mvc._
import scala.Some

trait S3Service {
  def download(bucket: String, fullKey: String, headers: Option[Headers] = None): Result

  def upload(bucket: String, keyName: String): BodyParser[String]

  def delete(bucket: String, keyName: String): DeleteResponse

}

object EmptyS3Service extends S3Service {
  def download(bucket: String, fullKey: String, headers: Option[Headers]): Result = ???

  def upload(bucket: String, keyName: String): BodyParser[String] = ???

  def delete(bucket: String, keyName: String): DeleteResponse = ???

}

class ConcreteS3Service(key: String, secret: String)(implicit actorSystem: ActorSystem) extends S3Service {

  import java.io.InputStream
  import log.Logger
  import play.api.http.HeaderNames._
  import play.api.mvc.Results._
  import scala.concurrent.duration._

  val duration = 10.seconds
  implicit val timeout: Timeout = Timeout(duration)
  type BytesIn = Array[Byte]

  private val client: AmazonS3Client = new AmazonS3Client(new AWSCredentials {
    def getAWSAccessKeyId: String = key

    def getAWSSecretKey: String = secret
  })

  def download(bucket: String, fullKey: String, headers: Option[Headers]): Result = {

    def nullOrEmpty(s: String) = s == null || s.isEmpty

    if (nullOrEmpty(fullKey) || nullOrEmpty(bucket)) {
      BadRequest("Invalid key")
    } else {

      def returnResultWithAsset(s3: AmazonS3Client, bucket: String, key: String): Result = {
        val s3Object: S3Object = s3.getObject(bucket, fullKey) //get object. may result in exception
        val inputStream: InputStream = s3Object.getObjectContent
        val objContent: Enumerator[Array[Byte]] = Enumerator.fromStream(inputStream)
        val metadata = s3Object.getObjectMetadata
        SimpleResult(
          header = ResponseHeader(200, Map(CONTENT_LENGTH.toString -> metadata.getContentLength.toString, ETAG -> metadata.getETag)),
          body = objContent
        )
      }

      def returnNotModifiedOrResultWithAsset(s3: AmazonS3Client, headers: Headers, bucket: String, key: String): Result = {
        val metadata: ObjectMetadata = s3.getObjectMetadata(new GetObjectMetadataRequest(bucket, fullKey))
        val ifNoneMatch = headers.get(IF_NONE_MATCH).getOrElse("")
        if (ifNoneMatch != "" && ifNoneMatch == metadata.getETag) Results.NotModified else returnResultWithAsset(s3, bucket, fullKey)
      }

      try {
        headers match {
          case Some(foundHeaders) => returnNotModifiedOrResultWithAsset(client, foundHeaders, bucket, fullKey)
          case _ => returnResultWithAsset(client, bucket, fullKey)
        }
      }
      catch {
        case e: AmazonClientException =>
          Logger.error("AmazonClientException in s3download: " + e.getMessage)
          BadRequest("Error downloading")
        case e: AmazonServiceException =>
          Logger.error("AmazonServiceException in s3download: " + e.getMessage)
          BadRequest("Error downloading")
      }
    }
  }


  def nothing = Done[Array[Byte], Either[Result, String]](Left(BadRequest("?")), Input.Empty)

  def upload(bucket: String, keyName: String): BodyParser[String] = BodyParser("S3Service") {

    request =>
      request.headers.get(CONTENT_LENGTH).map(_.toInt).map {
        l =>
          Logger.debug("Begin upload to: " + bucket + " " + keyName)

          val outputStream = new PipedOutputStream()

          val ref = actorSystem.actorOf(Props(new S3Writer(client, bucket, keyName, new PipedInputStream(outputStream), l)))
          ref ! Begin

          val out: Iteratee[BytesIn, Int] = {
            Iteratee.fold[BytesIn, Int](0) {
              (length, bytes) =>
                outputStream.write(bytes, 0, bytes.size)
                length + bytes.size
            }
          }
          out.mapDone({
            i =>
              outputStream.close()
              ref ! EOF
              Right(keyName)
          })
      }.getOrElse(nothing)
  }


  def delete(bucket: String, keyName: String): DeleteResponse = {
    Logger.info("S3Service.delete: %s, %s".format(bucket, keyName))
    try {
      val s3obj: S3Object = client.getObject(bucket, keyName) //get object. may result in exception
      client.deleteObject(bucket, s3obj.getKey)
      DeleteResponse(true, keyName)
    } catch {
      case e: AmazonClientException =>
        DeleteResponse(success = false, keyName, e.getMessage)
      case e: AmazonServiceException =>
        DeleteResponse(success = false, keyName, e.getMessage)
    }
  }
}
