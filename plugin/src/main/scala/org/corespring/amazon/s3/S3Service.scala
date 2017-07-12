package org.corespring.amazon.s3

import java.io.{IOException, PipedInputStream, PipedOutputStream}

import akka.util.Timeout
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.model.{GetObjectMetadataRequest, ObjectMetadata, PutObjectResult, S3Object}
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client, S3ClientOptions}
import com.amazonaws.{AmazonClientException, AmazonServiceException}
import org.corespring.amazon.s3.models._
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.iteratee.{Done, _}
import play.api.mvc._

import scala.concurrent._

trait S3Service {
  def download(bucket: String, fullKey: String, headers: Option[Headers] = None): SimpleResult

  def upload(bucket: String, keyName: String, predicate: (RequestHeader => Option[SimpleResult]) = (r => None)): BodyParser[Int]

  def delete(bucket: String, keyName: String): DeleteResponse

  def uploadWithData[A](bucket: String, makeKey: A => String)(predicate: RequestHeader => Either[SimpleResult, A]): BodyParser[Future[(Uploaded, A)]]

}

object EmptyS3Service extends S3Service {

  override def download(bucket: String, fullKey: String, headers: Option[Headers]): SimpleResult = ???

  override def upload(bucket: String, keyName: String, predicate: (RequestHeader => Option[SimpleResult])): BodyParser[Int] = ???

  override def delete(bucket: String, keyName: String): DeleteResponse = ???

  override def uploadWithData[A](bucket: String, makeKey: A => String)(predicate: RequestHeader => Either[SimpleResult, A]): BodyParser[Future[(Uploaded, A)]] = ???
}

object S3Service {

  def mkClient(key: String, secret: String, endpoint: Option[String] = None) = {
    val out = new AmazonS3Client(
      new AWSCredentials {
        def getAWSAccessKeyId: String = key
        def getAWSSecretKey: String = secret
      })

    endpoint.foreach { e =>
      out.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true))
      out.setEndpoint(e)
    }

    out
  }
}
class ConcreteS3Service(
  val client: AmazonS3,
  val transferManager: TransferManager /*key: String, secret: String, endpoint : Option[String]*/ ) extends S3Service {

  import java.io.InputStream

  import log.Logger
  import play.api.http.HeaderNames._
  import play.api.mvc.Results._

  import scala.concurrent.duration._

  val duration = 10.seconds
  implicit val timeout: Timeout = Timeout(duration)

  override def download(bucket: String, fullKey: String, headers: Option[Headers]): SimpleResult = {

    Logger.debug(s"[download] $bucket, $fullKey")
    Logger.trace(s"[download] $headers")

    def nullOrEmpty(s: String) = s == null || s.isEmpty

    if (nullOrEmpty(fullKey) || nullOrEmpty(bucket)) {
      BadRequest("Invalid key")
    } else {
      def returnResultWithAsset(bucket: String, key: String): SimpleResult = {

        val s3Object: S3Object = client.getObject(bucket, fullKey) //get object. may result in exception
        val inputStream: InputStream = s3Object.getObjectContent
        val objContent: Enumerator[Array[Byte]] = Enumerator.fromStream(inputStream)
        val metadata = s3Object.getObjectMetadata
        val contentType = metadata.getContentType()
        SimpleResult(
          header = ResponseHeader(200, Map(
            CONTENT_TYPE -> (if (contentType != null) contentType else "application/octet-stream"),
            CONTENT_LENGTH.toString -> metadata.getContentLength.toString,
            ETAG -> metadata.getETag)),
          body = objContent)
      }

      def returnNotModifiedOrResultWithAsset(headers: Headers, bucket: String, key: String): SimpleResult = {
        val metadata: ObjectMetadata = client.getObjectMetadata(new GetObjectMetadataRequest(bucket, fullKey))
        val ifNoneMatch = headers.get(IF_NONE_MATCH).getOrElse("")
        if (ifNoneMatch != "" && ifNoneMatch == metadata.getETag) Results.NotModified else returnResultWithAsset(bucket, fullKey)
      }

      try {
        headers match {
          case Some(foundHeaders) => returnNotModifiedOrResultWithAsset(foundHeaders, bucket, fullKey)
          case _ => returnResultWithAsset(bucket, fullKey)
        }
      } catch {
        case e: AmazonClientException =>
          Logger.error(s"AmazonClientException in s3download for bucket: $bucket, key: $fullKey: " + e.getMessage)
          BadRequest("Error downloading")
        case e: AmazonServiceException =>
          Logger.error(s"AmazonClientException in s3download for bucket: $bucket, key: $fullKey: " + e.getMessage)
          BadRequest("Error downloading")
      }
    }
  }

  private def emptyPredicate(r: RequestHeader): Option[SimpleResult] = {
    Logger.debug("Empty Predicate - return None")
    None
  }

  @deprecated("Use s3Object instead", "0.5")
  override def upload(bucket: String, keyName: String, predicate: (RequestHeader => Option[SimpleResult]) = emptyPredicate): BodyParser[Int] = BodyParser("S3Service") {

    request =>

      def nothing(msg: String, cleanupFn: () => Unit = () => ()): play.api.libs.iteratee.Iteratee[Array[Byte], Either[play.api.mvc.SimpleResult, Int]] = {
        cleanupFn()
        Logger.error("S3Service.upload: " + msg)
        Done[Array[Byte], Either[SimpleResult, Int]](Left(BadRequest(msg)), Input.Empty)
      }

      def uploadValidated: Iteratee[Array[Byte], Either[SimpleResult, Int]] = {
        request.headers.get(CONTENT_LENGTH).map(_.toInt).map {
          contentLength =>
            Logger.debug("[uploadValidated] Begin upload to: " + bucket + " " + keyName)
            val outputStream = new PipedOutputStream()
            val inputStream = new PipedInputStream()
            def closeStreams() = {
              try {
                outputStream.close(); inputStream.close()
              } catch {
                case e: IOException =>
              }
            }
            def writeError(e: Throwable): SimpleResult = {
              closeStreams()
              Logger.error("S3Service.upload: could not write to stream: " + e.getMessage)
              BadRequest(e.getMessage)
            }
            try {
              val inputStream = new PipedInputStream(outputStream)
              val objectMetadata = new ObjectMetadata
              objectMetadata.setContentLength(contentLength)
              val s3uploader: Future[Either[Exception, PutObjectResult]] = future {
                try {
                  //this will block until all data is piped
                  Right(client.putObject(bucket, keyName, inputStream, objectMetadata))
                } catch {
                  case e: Exception => Left(e)
                }
              }
              //this code is copied from the Iteratee.foldM source code in play.api.libs.iteratee.Iteratee
              def step(result: Either[SimpleResult, Int])(input: Input[Array[Byte]]): Iteratee[Array[Byte], Either[SimpleResult, Int]] = input match {
                case Input.EOF => try {
                  Await.result(s3uploader, duration) match {
                    case Right(putObjectResult) => Done(result, Input.EOF)
                    case Left(e) => {
                      Logger.error("error occurred on s3 upload: " + e.getMessage)
                      Error("error occurred on s3 upload", Input.EOF)
                    }
                  }
                } catch {
                  case e: TimeoutException => Error("uploader timed out", Input.EOF)
                }
                case Input.Empty => {
                  Logger.trace("input is empty")
                  Cont(i => step(result)(i))
                }
                case Input.El(bytes) => {
                  Logger.trace(s"bytes received..")
                  val f = future[Either[SimpleResult, Int]] {
                    result match {
                      case Left(_) => result //an error occured, don't write anymore
                      case Right(total) => try {
                        Logger.trace(s"bytes total.. ${total}")
                        outputStream.write(bytes, 0, bytes.size)
                        Right(total + bytes.size)
                      } catch {
                        case e: IOException => Left(writeError(e))
                      }
                    }
                  }
                  Iteratee.flatten(f.map(r => Cont(i => step(r)(i))))
                }
              }
              (Cont[Array[Byte], Either[SimpleResult, Int]](i => step(Right(0))(i)))
            } catch {
              case e: AmazonServiceException => nothing(e.getMessage, closeStreams)
              case e: AmazonClientException => nothing(e.getMessage, closeStreams)
              case e: IOException => nothing(e.getMessage, closeStreams)
            }

        }.getOrElse(nothing("no content length specified"))
      }

      predicate(request).map { r =>
        Logger.debug(s"Predicate failed - returning: $r")
        Done[Array[Byte], Either[SimpleResult, Int]](Left(r), Input.Empty)
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

  lazy val parser = new S3BodyParser {
    override def transferManager: TransferManager = ConcreteS3Service.this.transferManager

    override implicit def ec: ExecutionContext = ExecutionContext.Implicits.global
  }

  override def uploadWithData[A](bucket: String, makeKey: A => String)(predicate: (RequestHeader) => Either[SimpleResult, A]): BodyParser[Future[(Uploaded, A)]] = {
    parser.uploadWithDataMakeKey[A](bucket, makeKey)(predicate)
  }
}
