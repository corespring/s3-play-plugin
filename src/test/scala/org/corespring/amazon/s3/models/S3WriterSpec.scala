package org.corespring.amazon.s3.models

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.util.Timeout
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.typesafe.config.ConfigFactory
import java.io.{PipedOutputStream, PipedInputStream, InputStream}
import java.util.GregorianCalendar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import scala.concurrent.duration._

class S3WriterSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpec
  with MustMatchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("S3WriterSpec"))

  implicit val timeout: Timeout = Timeout(2.seconds)

  val testBucket = ConfigFactory.load().getString("testBucket")

  val client = new AmazonS3Client( new AWSCredentials {
    def getAWSAccessKeyId: String = ConfigFactory.load().getString("amazonKey")
    def getAWSSecretKey: String = ConfigFactory.load().getString("amazonSecret")
  })

  def toByteArray(s:InputStream) : Array[Byte] = Stream.continually(s.read).takeWhile(-1 !=).map(_.toByte).toArray

  override def beforeAll(){
    import scala.collection.JavaConversions._

    def hasBucket = client.listBuckets().toList.exists( _.getName == testBucket)

    if(!hasBucket){
      client.createBucket(testBucket)
    }
  }

  def testFileName =  new GregorianCalendar().getTimeInMillis + "-test-file.jpeg"

  override def afterAll() {
    //client.deleteObject(testBucket, testFileName)
    system.shutdown()
  }

  def createRef(c:AmazonS3Client, bucket:String, name:String) : (ActorRef,PipedOutputStream,Array[Byte]) = {
    val inputStream : InputStream = this.getClass.getResourceAsStream("/cute-squirrel.jpeg")
    val bytes : Array[Byte] = toByteArray(inputStream)
    val outputStream : PipedOutputStream = new PipedOutputStream()
    val pipedInputStream : PipedInputStream = new PipedInputStream(outputStream)
    (system.actorOf(Props(new S3Writer(c,bucket, name, pipedInputStream, bytes.length))), outputStream, bytes)
  }

  "An S3Writer actor" must {

    "upload the file" in {
      val name = testFileName
      val (ref,outputStream,bytes) = createRef(client, testBucket, name)
      ref ! Begin
      outputStream.write(bytes)
      ref ! EOF
      expectMsgAllOf(WriterReady, WriteCompleted)

      Thread.sleep(1000)
      val uploadedFile = client.getObject(testBucket,name)
      uploadedFile.getKey must equal(name)
    }

    "return an error if there was an error" in {
      val (ref,_,_) = createRef(client, "bad bucket", testFileName)
      ref ! Begin
      expectMsg(WriteError("An error occurred", null))
    }

    "return an error if the bucket doesnt exist" in {
      val (ref,_,_) = createRef(client, "bad-bucket", testFileName)
      ref ! Begin
      expectMsg(WriteError(S3Writer.Message.S3Error, "The specified bucket does not exist"))
    }

    "return an error if the credentials are wrong" in {
      val badClient = new AmazonS3Client( new AWSCredentials {
        def getAWSAccessKeyId: String = "blah"
        def getAWSSecretKey: String = "blah"
      })
      val (ref,_,_) = createRef(badClient, "bad-bucket", testFileName)
      ref ! Begin
      expectMsg(WriteError(S3Writer.Message.S3Error, "The AWS Access Key Id you provided does not exist in our records."))
    }
  }
}


