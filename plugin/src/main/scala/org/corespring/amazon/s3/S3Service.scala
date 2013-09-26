package org.corespring.amazon.s3

import akka.util.Timeout
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{GetObjectMetadataRequest, ObjectMetadata, S3Object}
import com.amazonaws.{AmazonServiceException, AmazonClientException}
import java.io.{IOException, PipedInputStream, PipedOutputStream}
import org.corespring.amazon.s3.models._
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.iteratee._
import play.api.mvc._
import scala.Some
import scala.concurrent._

import play.api.libs.iteratee.Done
import java.util.concurrent.TimeUnit

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

class ConcreteS3Service(key: String, secret: String) extends S3Service {

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

      def nothing(msg:String, cleanupFn:() => Unit = ()=>()) = {
        cleanupFn()
        Logger.error("S3Service.upload: "+msg)
        Done[Array[Byte], Either[Result, String]](Left(BadRequest(msg)), Input.Empty)
      }

      def uploadValidated = {
        request.headers.get(CONTENT_LENGTH).map(_.toInt).map {
          contentLength =>
            Logger.debug("[uploadValidated] Begin upload to: " + bucket + " " + keyName)
            val outputStream = new PipedOutputStream()
            var inputStream = new PipedInputStream()
            def closeStreams() = {
              try {
                outputStream.close(); inputStream.close()
              }catch{
                case e:IOException =>
              }
            }
            def writeError(e:Throwable):Result = {
              closeStreams()
              Logger.error("S3Service.upload: could not write to stream: "+e.getMessage)
              BadRequest(e.getMessage)
            }
            try {
              val inputStream = new PipedInputStream(outputStream)
              val objectMetadata = new ObjectMetadata
              objectMetadata.setContentLength(contentLength)
              future{
                try{
                  Await.result(future{
                    //this will block until all data is piped
                    client.putObject(bucket, keyName, inputStream, objectMetadata)
                    Logger.debug("S3Service.upload: completed upload")
                  }, duration)
                } catch {
                  case e:Exception => {
                    Logger.error("error occured during upload: "+e.getMessage)
                    closeStreams()
                  }
                }
              }
              Iteratee.foldM[Array[Byte], Either[Result,String]](Right(keyName))((result,bytes) => {
                future[Either[Result,String]] {
                  result match {
                    case Left(_) => result //an error occured, don't write anymore
                    case Right(_) => try{
                      outputStream.write(bytes, 0, bytes.size)
                      result
                    } catch {
                      case e:IOException => Left(writeError(e))
                    }
                  }
                }
              })

            } catch {
              case e: AmazonServiceException => nothing(e.getMessage, closeStreams)
              case e: AmazonClientException => nothing(e.getMessage, closeStreams)
              case e: IOException => nothing(e.getMessage, closeStreams)
            }

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
