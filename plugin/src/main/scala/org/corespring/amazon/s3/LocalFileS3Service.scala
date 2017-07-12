package org.corespring.amazon.s3

import java.io.{InputStream, FileOutputStream, File}
import java.net.URL
import java.util
import java.util.Date

import com.amazonaws.{regions, AmazonWebServiceRequest, HttpMethod}
import com.amazonaws.services.s3.{S3ResponseMetadata, S3ClientOptions, AmazonS3}
import com.amazonaws.services.s3.model._
import org.corespring.amazon.s3.log._
import org.corespring.amazon.s3.models.DeleteResponse
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import play.api.Play
import play.api.libs.MimeTypes
import play.api.libs.iteratee.{Done, Enumerator}
import play.api.mvc._
import play.utils.UriEncoding

import scala.concurrent.Future

class LocalFileS3Service() extends S3Service{

  val logger = play.api.Logger(this.getClass)

  import play.api.Play.current
  import scala.concurrent.ExecutionContext.Implicits.global

  val rootPath = "target/.s3-local-file-service"

  lazy val generatedFolder: File = {
    val f = Play.getFile(rootPath)

    if (!f.exists) {
      f.mkdirs()
    } else if (f.isFile) {
      f.delete()
      f.mkdir()
    }
    f
  }

  require(generatedFolder.exists())
  require(generatedFolder.canWrite)

  private def resourceNameAt(path: String, file: String): Option[String] = {
    val decodedFile = UriEncoding.decodePath(file, "utf-8")
    val resourceName = s"$rootPath/$path/$decodedFile"
    logger.debug(s"resourceName: $resourceName")
    if (new File(resourceName).isDirectory) {
      logger.debug(s"for $resourceName return None")
      None
    } else {
      Some(resourceName)
    }
  }

  private val timeZoneCode = "GMT"

  //Dateformatter is immutable and threadsafe
  private val df: DateTimeFormatter =
    DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss '" + timeZoneCode + "'").withLocale(java.util.Locale.ENGLISH).withZone(DateTimeZone.forID(timeZoneCode))

  //Dateformatter is immutable and threadsafe
  private val dfp: DateTimeFormatter =
    DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss").withLocale(java.util.Locale.ENGLISH).withZone(DateTimeZone.forID(timeZoneCode))

  private val parsableTimezoneCode = " " + timeZoneCode

  private lazy val defaultCharSet = Play.configuration.getString("default.charset").getOrElse("utf-8")

  private def addCharsetIfNeeded(mimeType: String): String =
    if (MimeTypes.isText(mimeType))
      "; charset=" + defaultCharSet
    else ""

  import play.api.mvc.Results._

  def resource(p:String) : Option[URL] = {
    logger.debug(s"load resource: $p")

    val f = new File(p)

    if(f.exists) {
      logger.debug(s"found url...$p")
      Some(f.toURI.toURL)
    } else {
      logger.debug(s"can't find url...$p")
      None
    }
  }

  override def download(bucket: String, fullKey: String, headers: Option[Headers]): SimpleResult = {

    resourceNameAt(bucket, fullKey).map { resourceName =>

      resource(resourceName).map{ url =>

          lazy val (length, resourceData) = {
            val stream = url.openStream()
            try {
              (stream.available, Enumerator.fromStream(stream))
            } catch {
              case _: Throwable => (-1, Enumerator[Array[Byte]]())
            }
          }


          if (length == -1) {
            NotFound
          } else {

            import play.api.http.HeaderNames._
            import play.api.http.Status._
            import play.api.http.ContentTypes._

              // Prepare a streamed response
              SimpleResult(
                ResponseHeader(OK, Map(
                  CONTENT_LENGTH -> length.toString,
                  CONTENT_TYPE -> MimeTypes.forFileName(fullKey).map(m => m + addCharsetIfNeeded(m)).getOrElse(BINARY),
                  DATE -> df.print({ new java.util.Date }.getTime))),
                resourceData)
            }
        }
    }
  }.flatten.getOrElse(NotFound)

  override def uploadWithData[A](bucket: String, makeKey: (A) => String)(predicate: (RequestHeader) => Either[SimpleResult, A]): BodyParser[Future[(Uploaded, A)]] = {

    BodyParser("local-file-s3-object") { request =>
      predicate(request) match {
        case Left(result) => {
          Logger.debug(s"Predicate failed - returning: ${result.header.status}")
          Done[Array[Byte], Either[SimpleResult,Future[(Uploaded,A)]]](Left(result))
        }
        case Right(data) => {
          val key = makeKey(data)

          BodyParsers.parse.raw(request).mapM[Either[SimpleResult,Future[(Uploaded,A)]]]{
            case Left(r) => Future(Left(r))
            case Right(buffer) => {
              bytesToFile(s"${generatedFolder.getPath}/$bucket/$key", buffer.asBytes().get)
              Future(Right(Future(Uploaded("bucket", key), data)))
            }
          }
        }
        }
    }
  }

  private def bytesToFile(path:String, bytes:Array[Byte]) = {
    logger.debug(s"bytes to file ...., $path")
    val file = new File(path)
    if(!file.exists()){
      logger.debug(s"--> create parent dirs...${file.getParentFile}, ${file.getParentFile.getAbsolutePath}")
      file.getParentFile.mkdirs()
    }
    val fos = new FileOutputStream(file)
    fos.write(bytes)
  }

  override def upload(bucket: String, keyName: String, predicate: (RequestHeader) => Option[SimpleResult]): BodyParser[Int] = {
    uploadWithData[Int](bucket, i => keyName)((rh) => Right(0)).map{ (f:Future[(Uploaded,Int)]) =>
      0
    }
  }

  override def delete(bucket: String, keyName: String): DeleteResponse = DeleteResponse(true, keyName)

}
