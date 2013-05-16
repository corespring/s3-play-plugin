package org.corespring.amazon.s3.models

import akka.actor.Actor
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{AmazonS3Exception, ObjectMetadata}
import java.io.InputStream
import akka.event.Logging

case object Begin

case object EOF

case object WriteCompleted

case object WriterReady

case class WriteError(msg: String, t: String)

private object S3Writer{
  object Message{
    val GeneralError = "An error occurred"
    val S3Error = "An S3 exception occurred"

  }
}

private[s3] class S3Writer(client: AmazonS3Client, bucket: String, keyName: String, inputStream: InputStream, contentLength: Int) extends Actor {

  import S3Writer.Message._

  val log = Logging(context.system, this)

  log.debug("inputStream: " + inputStream)

  def receive = {
    case Begin => begin match {
      case Right(ready) => sender ! ready
      case Left(e) => {
        inputStream.close()
        sender ! e
      }
    }
    case EOF => end match {
      case Right(complete) => {
        log.debug("[EOF] return complete: " + complete + " to sender")
        sender ! complete
      }
      case Left(we) => {
        log.warning("[EOF] Writer error on end....")
        sender ! we
      }
    }
    case _ => throw new RuntimeException("Unknown command")
  }

  private def begin: Either[WriteError, WriterReady.type] = try {
    context.system.log.debug("Begin upload...")
    val objectMetadata = new ObjectMetadata
    objectMetadata.setContentLength(contentLength)
    client.putObject(bucket, keyName, inputStream, objectMetadata)
    Right(WriterReady)
  } catch {
    case e: IllegalArgumentException => Left(WriteError(GeneralError, e.getMessage))
    case awse : AmazonS3Exception => Left(WriteError(S3Error, awse.getMessage))
  }

  private def end: Either[WriteError, WriteCompleted.type] = try {
    log.debug("[end] - close the input stream: " + inputStream)
    inputStream.close()
    Right(WriteCompleted)
  } catch {
    case e: Throwable => Left(WriteError(GeneralError, e.getMessage))
  }
}
