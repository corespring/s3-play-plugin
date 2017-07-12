package org.corespring.amazon.s3

import java.io.InputStream
import java.util.GregorianCalendar
import java.util.concurrent.{TimeUnit, TimeoutException}

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.transfer.TransferManager
import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification
import play.api.http.HeaderNames._
import play.api.libs.iteratee.{Enumerator, Iteratee}
import com.amazonaws.services.s3.transfer.TransferManager
import org.specs2.time.NoTimeConversions
import play.api.mvc._
import play.api.test.{FakeHeaders, FakeRequest}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class BodyParserSpec extends Specification with NoTimeConversions {

  val key = ConfigFactory.load().getString("amazonKey")
  val secret = ConfigFactory.load().getString("amazonSecret")
  val bucket = ConfigFactory.load().getString("testBucket")

  def mkFilename = new GregorianCalendar().getTimeInMillis + "-s3-writer-spec-file.jpeg"

  lazy val s3Parser = new S3BodyParser {
    override implicit def ec: ExecutionContext = ExecutionContext.Implicits.global

    val client: AmazonS3Client = new AmazonS3Client(new AWSCredentials {
      override def getAWSAccessKeyId: String = key
      override def getAWSSecretKey: String = secret
    })

    override val transferManager = new TransferManager(client)
  }

  def upload(byteArray: Array[Byte], filename: String, bucket:String = bucket): Either[Result, Future[(Uploaded, Unit)]] = {
    val request: Request[AnyContent] = FakeRequest("?", "?",
      FakeHeaders(Seq(CONTENT_LENGTH.toString -> Seq(byteArray.size.toString))),
      AnyContentAsRaw(RawBuffer(byteArray.size, byteArray)))
    val enumerator = Enumerator[Array[Byte]](byteArray)
    val parser: BodyParser[Future[(Uploaded, Unit)]] = s3Parser.uploadWithData[Unit](bucket, filename)(rh => Right(Unit))
    val iteratee: Iteratee[Array[Byte], Either[Result, Future[(Uploaded, Unit)]]] = parser.apply(request)
    Await.result(enumerator.run(iteratee), 10.seconds)
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
          s3.key === name
          s3.bucket === bucket
          val s3o = s3Parser.transferManager.getAmazonS3Client.getObject(s3.bucket, s3.key)
          toByteArray(s3o.getObjectContent) === byteArray
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

    "doesn't return a hanging future" in {
      val ba : Array[Byte]= toByteArray(inputStream)
      val result = upload(ba, "x@y.jpg", "bad-bucket" )
      result match {
        case Left(r) => {
          println(r)
          ko("we should be getting a future back")
        }
        case Right(f) => {
          try {
            val (s3o, _) = Await.result(f, 20.seconds)
            true must_== true
          } catch {
            case t: TimeoutException => ko("we shouldn't get a timeout exception")
            case t : RuntimeException => t.getMessage.startsWith(s"key=x@y.jpg") must_== true
          }
        }
      }
    }
  }
}
