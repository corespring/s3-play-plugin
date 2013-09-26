package org.corespring.amazon.s3

import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import java.io.InputStream
import java.util.GregorianCalendar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import play.api.http.HeaderNames._
import play.api.libs.iteratee.Input.EOF
import play.api.libs.iteratee.{Enumerator, Iteratee}
import play.api.mvc._
import play.api.test.FakeHeaders
import play.api.test.FakeRequest
import scala.concurrent.Await

class S3ServiceSpec extends WordSpec
with MustMatchers
with BeforeAndAfterAll {

  def testFileName = new GregorianCalendar().getTimeInMillis + "-s3-writer-spec-file.jpeg"

  "s3 service" must {
    "work" in {

      val key = ConfigFactory.load().getString("amazonKey")
      val secret = ConfigFactory.load().getString("amazonSecret")
      val bucket = ConfigFactory.load().getString("testBucket")
      val service = new ConcreteS3Service(key, secret)
      val filename = testFileName
      def toByteArray(s: InputStream): Array[Byte] = Stream.continually(s.read).takeWhile(-1 !=).map(_.toByte).toArray
      val inputStream: InputStream = this.getClass.getResourceAsStream("/pug.jpg")
      val byteArray = toByteArray(inputStream)
      val length: String = byteArray.size.toString

      val request: Request[AnyContent] = FakeRequest("?", "?",
        FakeHeaders(Seq(CONTENT_LENGTH.toString -> Seq(length))),
        AnyContentAsRaw(RawBuffer(byteArray.size, byteArray)))

      val enumerator = Enumerator[Array[Byte]](byteArray)
      val parser: BodyParser[Int] = service.upload(bucket, filename)
      val iteratee: Iteratee[Array[Byte], Either[Result, Int]] = parser.apply(request)


      import scala.concurrent.duration._
      val out = Await.result(enumerator.run(iteratee), 10.seconds)
      out.right.get must equal(byteArray.length)
    }
  }

}
