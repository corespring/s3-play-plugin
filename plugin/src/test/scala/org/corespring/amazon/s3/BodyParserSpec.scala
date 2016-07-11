package org.corespring.amazon.s3

import java.io.InputStream
import java.util.GregorianCalendar
import java.util.concurrent.TimeUnit

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.transfer.TransferManager
import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification
import play.api.http.HeaderNames._
import play.api.libs.iteratee.{ Enumerator, Iteratee }
import com.amazonaws.services.s3.transfer.TransferManager
import play.api.mvc._
import play.api.test.{ FakeHeaders, FakeRequest }

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext, Future }

class BodyParserSpec extends Specification {

  val key = ConfigFactory.load().getString("amazonKey")
  val secret = ConfigFactory.load().getString("amazonSecret")
  val bucket = ConfigFactory.load().getString("testBucket")

  def mkFilename = new GregorianCalendar().getTimeInMillis + "-s3-writer-spec-file.jpeg"

  lazy val s3Parser = new S3BodyParser {
    override implicit def ec: ExecutionContext = ExecutionContext.Implicits.global

    override val client: AmazonS3Client = new AmazonS3Client(new AWSCredentials {
      override def getAWSAccessKeyId: String = key
      override def getAWSSecretKey: String = secret
    })

    override val transferManager = new TransferManager(client)
  }

  def upload(byteArray: Array[Byte], filename: String): Either[Result, Future[(S3Object, Unit)]] = {
    val request: Request[AnyContent] = FakeRequest("?", "?",
      FakeHeaders(Seq(CONTENT_LENGTH.toString -> Seq(byteArray.size.toString))),
      AnyContentAsRaw(RawBuffer(byteArray.size, byteArray)))
    val enumerator = Enumerator[Array[Byte]](byteArray)
    val parser: BodyParser[Future[(S3Object, Unit)]] = s3Parser.s3ObjectAndData[Unit](bucket, filename)(rh => Right(Unit))
    val iteratee: Iteratee[Array[Byte], Either[Result, Future[(S3Object, Unit)]]] = parser.apply(request)
    Await.result(enumerator.run(iteratee), Duration(10, TimeUnit.SECONDS))
  }

  def toByteArray(s: InputStream): Array[Byte] = Stream.continually(s.read).takeWhile(-1 !=).map(_.toByte).toArray
  val inputStream: InputStream = this.getClass.getResourceAsStream("/cute-squirrel.jpeg")
  val byteArray = toByteArray(inputStream)

  "parser" should {
    "work" in {
      val name = mkFilename
      val result = upload(byteArray, name)

      result match {
        case Left(result) => failure("should not get result")
        case Right(futureS3) => {
          val (s3, _) = Await.result(futureS3, Duration(10, TimeUnit.SECONDS))
          println(s3)
          s3.getKey === name
          s3.getBucketName === bucket
          toByteArray(s3.getObjectContent) === byteArray
          true === true
        }
      }
    }

    "upload multiple" in {
      (1 until 20).foreach { index =>
        val name = mkFilename
        val result = upload(byteArray, name)
      }
      true === true
    }
  }
}
