package org.corespring.amazon.s3.models

import akka.actor.Actor
import akka.event.Logging
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import java.io.InputStream
import scala.concurrent.Future

case object Begin
case class BeginResult(success:Boolean, error : Option[String] = None)

case object Complete

case class WriteResult(errors: Seq[String])

private object S3Writer {

  object Message {
    val GeneralError = "An error occurred"
    val S3Error = "An S3 exception occurred"
  }

}

private[s3] class S3Writer(client: AmazonS3Client, bucket: String, keyName: String, inputStream: InputStream, contentLength: Int) extends Actor {

  import akka.pattern._
  import context.dispatcher

  var errors: Seq[Throwable] = Seq()

  val log = Logging(context.system, this)

  log.debug(">> inputStream: " + inputStream)

  def receive = {
    /**
     * When we call begin - noone is going to listen to the response.
     * This is because nothing happens until we've started putting data into the stream
     * To allow the sender to get information about the success/failure we'll story any errors
     * in a map
     */
    case Begin => Future {
      try {
        log.debug(s"Begin upload: $bucket name: $keyName")
        val objectMetadata = new ObjectMetadata
        objectMetadata.setContentLength(contentLength)
        log.debug(s"content length: $contentLength")
        val result = client.putObject(bucket, keyName, inputStream, objectMetadata)
        log.debug("client result: " + result)
        BeginResult(true)
      } catch {
        case e: Throwable => BeginResult(false, Some(if(e.getMessage == null) S3Writer.Message.GeneralError else e.getMessage))
      }
    }.pipeTo(sender)

    case Complete => {
      try {
        log.debug("Close the input stream")
        inputStream.close()
      }
      catch {
        case e: Throwable => errors :+ e
      }
      val msgs = errors.map(_.getMessage)
      log.debug(s"Send errors: $msgs")
      sender ! WriteResult(msgs)
    }
    case _ => throw new RuntimeException("Unknown command")
  }
}
