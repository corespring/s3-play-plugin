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
import scala.concurrent.Await

trait S3Service {
  def download(bucket: String, fullKey: String, headers: Option[Headers] = None): Result

  def upload(bucket: String, keyName: String, predicate: (RequestHeader => Option[Result]) = (r => None)): BodyParser[String]

  def delete(bucket: String, keyName: String): DeleteResponse

}

object EmptyS3Service extends S3Service {
  def download(bucket: String, fullKey: String, headers: Option[Headers]): Result = ???

  def upload(bucket: String, keyName: String, predicate: (RequestHeader => Option[Result])): BodyParser[String] = ???

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

  protected val client: AmazonS3Client = new AmazonS3Client(new AWSCredentials {
    def getAWSAccessKeyId: String = key

    def getAWSSecretKey: String = secret
  })

  def download(bucket: String, fullKey: String, headers: Option[Headers]): Result = {

    def nullOrEmpty(s: String) = s == null || s.isEmpty

    if (nullOrEmpty(fullKey) || nullOrEmpty(bucket)) {
      BadRequest("Invalid key")
    } else {

      def returnResultWithAsset(bucket: String, key: String): Result = {
        val s3Object: S3Object = client.getObject(bucket, fullKey) //get object. may result in exception
        val inputStream: InputStream = s3Object.getObjectContent
        val objContent: Enumerator[Array[Byte]] = Enumerator.fromStream(inputStream)
        val metadata = s3Object.getObjectMetadata
        SimpleResult(
          header = ResponseHeader(200, Map(CONTENT_LENGTH.toString -> metadata.getContentLength.toString, ETAG -> metadata.getETag)),
          body = objContent
        )
      }

      def returnNotModifiedOrResultWithAsset(headers: Headers, bucket: String, key: String): Result = {
        val metadata: ObjectMetadata = client.getObjectMetadata(new GetObjectMetadataRequest(bucket, fullKey))
        val ifNoneMatch = headers.get(IF_NONE_MATCH).getOrElse("")
        if (ifNoneMatch != "" && ifNoneMatch == metadata.getETag) Results.NotModified else returnResultWithAsset(bucket, fullKey)
      }

      try {
        headers match {
          case Some(foundHeaders) => returnNotModifiedOrResultWithAsset(foundHeaders, bucket, fullKey)
          case _ => returnResultWithAsset(bucket, fullKey)
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

  private def emptyPredicate( r : RequestHeader) : Option[Result] = {
    Logger.debug("Empty Predicate - return None")
    None
  }

  def upload(bucket: String, keyName: String, predicate: (RequestHeader => Option[Result]) = emptyPredicate): BodyParser[String] = BodyParser("S3Service") {

    request =>

      def nothing(msg:String) = Done[Array[Byte], Either[Result, String]](Left(BadRequest(msg)), Input.Empty)

      import akka.pattern._


      def uploadValidated = {
        request.headers.get(CONTENT_LENGTH).map(_.toInt).map {
          l =>
            Logger.debug("[uploadValidated] Begin upload to: " + bucket + " " + keyName)

            val outputStream = new PipedOutputStream()

            val ref = actorSystem.actorOf(Props(new S3Writer(client, bucket, keyName, new PipedInputStream(outputStream), l)))

            //We fire and forget here as we only check for errors at the end
            ref ! Begin

            val out: Iteratee[Array[Byte], Int] = {
              Iteratee.fold[Array[Byte], Int](0) {
                (length, bytes) =>
                  outputStream.write(bytes, 0, bytes.size)
                  length + bytes.size
              }
            }
            out.mapDone({
              i =>
                Logger.debug("[uploadValidated] mapDone")
                outputStream.close()
                val result = Await.result(ref ? Complete, 1.second)
                result match {
                  case WriteResult(Seq()) => {
                    Logger.debug("[uploadValidated] No errors returned - return keyName")
                    Right(keyName)
                  }
                  case WriteResult(errors) => Left(BadRequest("Some errors occured: " + errors.mkString("\n")))
                }
            })
        }.getOrElse(nothing("no content length specified"))
      }

      predicate(request).map { r =>
        Logger.debug(s"Predicate failed - returning: $r")
        Done[Array[Byte], Either[Result, String]](Left(r), Input.Empty)
      }.getOrElse(uploadValidated)


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
