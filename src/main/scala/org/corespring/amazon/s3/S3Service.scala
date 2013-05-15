package org.corespring.amazon.s3

import play.api.mvc._
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.model.{GetObjectMetadataRequest, ObjectMetadata, S3Object}
import play.api.libs.iteratee.{Iteratee, Input, Done, Enumerator}
import scala.Some
import play.api.mvc.SimpleResult
import org.corespring.amazon.s3.models._
import com.amazonaws.{AmazonServiceException, AmazonClientException}
import java.io.{IOException, PipedInputStream, PipedOutputStream}
import play.api.libs.concurrent.Akka
import akka.actor.Props
import scala.concurrent.Future
import scala.Some
import play.api.mvc.SimpleResult
import play.api.mvc.ResponseHeader
import org.corespring.amazon.s3.models.DeleteResponse

trait S3Service {
  def download(bucket: String, fullKey: String, headers: Option[Headers] = None): Result

  def upload(bucket: String, keyName: String): BodyParser[Int]

  def delete(bucket: String, keyName: String): DeleteResponse

  def bucket: String
}

object EmptyS3Service extends S3Service {
  def download(bucket: String, fullKey: String, headers: Option[Headers]): Result = ???

  def upload(bucket: String, keyName: String): BodyParser[Int] = ???

  def delete(bucket: String, keyName: String): DeleteResponse = ???

  def bucket: String = ???
}

class ConcreteS3Service(key: String, secret: String, val bucket: String) extends S3Service {

  import play.api.http.HeaderNames._
  import java.io.InputStream
  import log.Logger
  import play.api.mvc.Results._

  private val client: AmazonS3Client = new AmazonS3Client(new AWSCredentials {
    def getAWSAccessKeyId: String = key

    def getAWSSecretKey: String = secret
  })

  def download(bucket: String, fullKey: String, headers: Option[Headers]): Result = {

    require(fullKey != null && !fullKey.isEmpty, "Invalid key")
    require(bucket != null && !bucket.isEmpty, "Invalid bucket")

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
      if (ifNoneMatch != "" && ifNoneMatch == metadata.getETag) {
        Results.NotModified
      }
      else {
        returnResultWithAsset(s3, bucket, fullKey)
      }
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

  private def nothing = Done[Array[Byte], Either[Result, Int]](Left(BadRequest("no Content-Length specified")), Input.Empty)

  def upload(bucket: String, keyName: String): BodyParser[Int] = BodyParser("s3 file upload") {
    request =>
      request.headers.get(CONTENT_LENGTH).map(_.toInt).map {
        l =>
          nothing
        //s3UploadSingle(bucket, keyName, l)
      }.getOrElse(nothing)
  }

  /**
   * create an actor for the s3 upload. pipe the data as it comes in to the s3 actor.
   * @param bucket the name of the amazon bucket
   * @param keyName
   * @return
   */
  private def s3UploadSingle(bucket: String, keyName: String, contentLength: Int): Iteratee[Array[Byte], Either[Result, Int]] = {
    Logger.debug("S3Service.s3UploadSingle bucket: " + bucket + " keyName: " + keyName)

    val outputStream = new PipedOutputStream()

    val ref = Akka.system.actorOf(Props(new S3Writer(client, bucket, keyName, new PipedInputStream(outputStream), contentLength)))
    ref ! Begin

    def processChunks(result: Either[Result, Int], bytes: Array[Byte]) = result match {
      case Right(acc) => outputStream.write(bytes, 0, bytes.size); Right(acc + bytes.size)
      case Left(e) => Left(e)
    }

    import akka.pattern._

    def onDone(result: Either[Result, Int]) = {
      outputStream.close()
      result match {
        case Right(acc) => (ref ? EOF).map { case WriteCompleted => Right(acc) }
        case Left(error) => Left(error)
      }
    }
      Iteratee.fold[Array[Byte], Either[Result, Int]](Right(0))(processChunks).mapDone(onDone)
    }



    def delete(bucket: String, keyName: String): DeleteResponse = ???

  }

/*

object ConcreteS3Service extends S3Service {

  def bucket = ConfigLoader.get("AMAZON_ASSETS_BUCKET").get

  private var optS3: Option[AmazonS3Client] = None


  def online:Boolean = {
    optS3 match {
      case Some(s3) => try{
        s3.listBuckets().size() > 0
      } catch {
        case e:AmazonClientException => false
        case e:AmazonServiceException => false
      }
      case None => false
    }
  }

  /**
   * Init the S3 client
   */
  def init: Unit = this.synchronized {
    try {
      optS3 = Some(new AmazonS3Client(new AWSCredentials {
        def getAWSAccessKeyId: String = ConfigFactory.load().getString("AMAZON_ACCESS_KEY")

        def getAWSSecretKey: String = ConfigFactory.load().getString("AMAZON_ACCESS_SECRET")
      }))
    } catch {
      case e: IOException => InternalError("unable to authenticate s3 server with given credentials", LogType.printFatal)
    }
  }


  /**
   * handle file upload through multiple parts. See http://docs.amazonwebservices.com/AmazonS3/latest/dev/llJavaUploadFile.html
   * handle the upload stream using an iteratee using the BodyParser. See https://github.com/playframework/Play20/wiki/ScalaBodyParsers
   * @param bucket the name of the amazon bucket
   * @param keyName
   * @return
   */
  override def s3upload(bucket: String, keyName: String): BodyParser[Int] = BodyParser("s3 file upload") {
    request =>
      val optContentLength = request.headers.get(CONTENT_LENGTH)
      optContentLength match {
        case Some(contentLength) => try {
          s3UploadSingle(bucket, keyName, contentLength.toInt)
        } catch {
          case e: Exception => Done[Array[Byte], Either[Result, Int]](Left(Results.InternalServerError(Json.toJson(ApiError.ContentLength(Some("value in Content-Length not valid integer"))))), Input.Empty)
        }
        case None => Done[Array[Byte], Either[Result, Int]](Left(Results.InternalServerError(Json.toJson(ApiError.ContentLength(Some("no Content-Length specified"))))), Input.Empty)
      }
  }

  /**
   * @return
   */
  override def s3download(bucket: String, itemId: String, keyName: String): Result = download(bucket, itemId + "/" + keyName)

  def download(bucket: String, fullKey: String, headers: Option[Headers] = None): Result = {

    require(fullKey != null && !fullKey.isEmpty, "Invalid key")
    require(bucket != null && !bucket.isEmpty, "Invalid bucket")

    def returnResultWithAsset(s3 : AmazonS3Client, bucket: String, key: String) : Result = {
      val s3Object: S3Object = s3.getObject(bucket, fullKey) //get object. may result in exception
      val inputStream: InputStream = s3Object.getObjectContent
      val objContent: Enumerator[Array[Byte]] = Enumerator.fromStream(inputStream)
      val metadata = s3Object.getObjectMetadata
      SimpleResult(
        header = ResponseHeader(200,
          Map(CONTENT_LENGTH -> metadata.getContentLength.toString,
            ETAG -> metadata.getETag)),
        body = objContent
      )
    }

    def returnNotModifiedOrResultWithAsset( s3 : AmazonS3Client, headers: Headers, bucket: String, key: String): Result = {
      val metadata: ObjectMetadata = s3.getObjectMetadata(new GetObjectMetadataRequest(bucket, fullKey))
      val ifNoneMatch = headers.get(IF_NONE_MATCH).getOrElse("")
      if (ifNoneMatch != "" && ifNoneMatch == metadata.getETag) {
        Results.NotModified
      }
      else {
        returnResultWithAsset(s3,bucket, fullKey)
      }
    }

    optS3 match {
      case Some(s3) => {
        try {
          headers match {
            case Some(foundHeaders) => returnNotModifiedOrResultWithAsset(s3, foundHeaders, bucket, fullKey)
            case _ => returnResultWithAsset(s3, bucket, fullKey)
          }
        }
        catch {
          case e: AmazonClientException =>
            Log.f("AmazonClientException in s3download: " + e.getMessage)
            Results.InternalServerError(Json.toJson(ApiError.AmazonS3Client(Some("Occurred when attempting to retrieve object: " + fullKey))))
          case e: AmazonServiceException =>
            Log.e("AmazonServiceException in s3download: " + e.getMessage)
            Results.InternalServerError(Json.toJson(ApiError.AmazonS3Server(Some("Occurred when attempting to retrieve object: " + fullKey))))
        }
      }
      case None =>
        Log.f("amazon s3 service not initialized")
        Results.InternalServerError(Json.toJson(ApiError.S3NotIntialized))
    }
  }


  override def delete(bucket: String, keyName: String): S3DeleteResponse = {

    Log.i("S3Service.delete: %s, %s".format(bucket, keyName))

    optS3 match {
      case Some(s3) => {
        try {
          val s3obj: S3Object = s3.getObject(bucket, keyName) //get object. may result in exception
          s3.deleteObject(bucket, s3obj.getKey())
          S3DeleteResponse(true, keyName)
        } catch {
          case e: AmazonClientException =>
            Log.f("AmazonClientException in delete: " + e.getMessage)
            S3DeleteResponse(false, keyName, e.getMessage)
          case e: AmazonServiceException =>
            Log.e("AmazonServiceException in delete: " + e.getMessage)
            S3DeleteResponse(false, keyName, e.getMessage)
        }
      }
      case _ => S3DeleteResponse(false, keyName, "Error with S3 Service")
    }
  }

  /**
   * create an actor for the s3 upload. pipe the data as it comes in to the s3 actor.
   * @param bucket the name of the amazon bucket
   * @param keyName
   * @param contentLength
   * @return
   */
  private def s3UploadSingle(bucket: String, keyName: String, contentLength: Int): Iteratee[Array[Byte], Either[Result, Int]] = {
    Log.i("S3Service.s3UploadSingle bucket: " + bucket + " keyName: " + keyName)

    optS3 match {
      case Some(s3) => {
        val outputStream = new PipedOutputStream()
        val s3Writer = new S3Writer(bucket, keyName, new PipedInputStream(outputStream), contentLength)
        s3Writer.start()
        try {
          s3Writer ! Begin //initiate upload. S3Writer will now wait for data chunks to be pushed to it's input stream
        } catch {
          case e: IOException => Log.f("error occurred when creating pipe")
        }
        Iteratee.fold[Array[Byte], Either[Result, Int]](Right(0)) {
          (result, chunk) =>
            result match {
              case Right(acc) => try {
                outputStream.write(chunk, 0, chunk.size)
                Right(acc + chunk.size)
              } catch {
                case e: IOException =>
                  Log.f("IOException occurred when writing to S3: " + e.getMessage)
                  Left(Results.InternalServerError(Json.toJson(ApiError.S3Write)))
              }
              case Left(error) => Left(error)
            }
        }.mapDone(result => {
          try {
            outputStream.close()
          } catch {
            case e: IOException =>
          }
          result match {
            case Right(acc) => {
              s3Writer !?(5000, EOF) match {
                case Some(Ack(s3reply)) => s3reply match {
                  case Right(_) => Right(acc)
                  case Left(error) => Left(Results.InternalServerError(Json.toJson(ApiError.S3Write(error.clientOutput))))
                }
                case None => Left(Results.InternalServerError(Json.toJson(ApiError.S3Write(Some("timeout occured before S3Writer could return")))))
                case _ => Left(Results.InternalServerError(Json.toJson(ApiError.S3Write(Some("unknown reply from S3Writer")))))
              }
            }
            case Left(error) => Left(error)
          }
        })
      }
      case None => Done[Array[Byte], Either[Result, Int]](Left(Results.InternalServerError("s3 instance not initialized")), Input.Empty)
    }
  }

  override def cloneFile(bucket: String, keyName: String, newKeyName:String) = {
    Log.d("S3Service Cloning "+keyName+" to "+newKeyName)
    optS3 match {
      case Some(s3) => s3.copyObject(bucket, keyName, bucket, newKeyName)
      case _ => throw new RuntimeException("Amazon S3 not initalized")
    }
  }

  private case object Begin

  private case object EOF

  private case class Ack(result: Either[InternalError, Unit])

  private class S3Writer(bucket: String, keyName: String, inputStream: InputStream, contentLength: Int) extends Actor {
    def act() {
      var errorOccurred: Option[InternalError] = None
      while (true) {
        receiveWithin(60000) {
          case Begin => {
            val objectMetadata = new ObjectMetadata;
            objectMetadata.setContentLength(contentLength)
            try {
              optS3.get.putObject(bucket, keyName, inputStream, objectMetadata) // assume optS3 has instance of S3 otherwise this would have never been called
            } catch {
              case e: Exception => {
                Log.f("exception occurred in Begin of S3Writer: " + e.getMessage)
                try {
                  inputStream.close()
                } catch {
                  case e: IOException => Log.f("IOException when closing input stream in S3Writer: " + e.getMessage)
                }
                errorOccurred = Some(InternalError("error writing to S3", LogType.printFatal))
              }
            }
          }
          case EOF => {
            try {
              inputStream.close()
            } catch {
              case e: IOException => Log.f("IOException when closing input stream in S3Writer: " + e.getMessage)
            }
            errorOccurred match {
              case Some(error) => Actor.reply(Ack(Left(error)))
              case None => Actor.reply(Ack(Right()));
            }
            exit()
          }
          case TIMEOUT => {
            try {
              inputStream.close()
            } catch {
              case e: IOException =>
            }
            exit()
          }
        }
      }
    }

    override def scheduler: IScheduler = new ResizableThreadPoolScheduler()
  }

}*/
