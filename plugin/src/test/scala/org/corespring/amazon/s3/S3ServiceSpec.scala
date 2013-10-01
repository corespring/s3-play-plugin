package org.corespring.amazon.s3

import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import java.io.InputStream
import java.util.GregorianCalendar
import models.DeleteResponse
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
import scala.concurrent.duration._
import play.api.http.Status._

class S3ServiceSpec extends WordSpec
with MustMatchers
with BeforeAndAfterAll {

  def testFileName = new GregorianCalendar().getTimeInMillis + "-s3-writer-spec-file.jpeg"

  def upload(byteArray:Array[Byte], service:ConcreteS3Service, bucket:String, filename: String):Int = {
    val request: Request[AnyContent] = FakeRequest("?", "?",
      FakeHeaders(Seq(CONTENT_LENGTH.toString -> Seq(byteArray.size.toString))),
      AnyContentAsRaw(RawBuffer(byteArray.size, byteArray)))
    val enumerator = Enumerator[Array[Byte]](byteArray)
    val parser: BodyParser[Int] = service.upload(bucket, filename)
    val iteratee: Iteratee[Array[Byte], Either[Result, Int]] = parser.apply(request)


    import scala.concurrent.duration._
    val out = Await.result(enumerator.run(iteratee), 10.seconds)
    out.right.getOrElse(-1)
  }

  def download(service:ConcreteS3Service, bucket:String, filename: String):Either[Int,Array[Byte]] = {
    service.download(bucket, filename) match {
      case SimpleResult(header,body) => if (header.status == OK){
        val consumer = Iteratee.fold[Array[Byte],Array[Byte]](Array())((output,input) => {
          output ++ input
        })
        Right(Await.result(Await.result(body.asInstanceOf[Enumerator[Array[Byte]]](consumer),10.seconds).run,10.seconds))
      }else{
        Left(header.status)
      }

      case _ => Left(-1)
    }
  }

  "s3 service" must {
    "upload, download, and delete" in {

      //set up
      val key = ConfigFactory.load().getString("amazonKey")
      val secret = ConfigFactory.load().getString("amazonSecret")
      val bucket = ConfigFactory.load().getString("testBucket")
      val service = new ConcreteS3Service(key, secret)
      val filename = testFileName
      def toByteArray(s: InputStream): Array[Byte] = Stream.continually(s.read).takeWhile(-1 !=).map(_.toByte).toArray
      val inputStream: InputStream = this.getClass.getResourceAsStream("/cute-squirrel.jpeg")
      val byteArray = toByteArray(inputStream)

      upload(byteArray,service,bucket,filename) must equal(byteArray.length)

      //download
      val downloadedBytes = download(service,bucket,filename).right.get
      def compareBytes:Boolean = {
        if(downloadedBytes.length == byteArray.length){
          var result = true
          for(i <- 0 to downloadedBytes.length-1){
            result && (downloadedBytes(i) == byteArray(i))
          }
          result
        } else false
      }
      compareBytes === true

      //delete
      val deleteResponse = service.delete(bucket,filename)
      deleteResponse.success === true
      download(service,bucket,filename).isLeft === true
    }
  }

}
