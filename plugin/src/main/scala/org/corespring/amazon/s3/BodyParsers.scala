package org.corespring.amazon.s3

import java.io.{ OutputStream, PipedInputStream, PipedOutputStream }

import com.amazonaws.event.{ ProgressEvent, ProgressEventType, ProgressListener }
import com.amazonaws.services.s3.model.{ ObjectMetadata  }
import com.amazonaws.services.s3.transfer.TransferManager
import play.api.libs.iteratee.{ Done, Iteratee }
import play.api.mvc.{ BodyParser, RequestHeader, SimpleResult }

import scala.concurrent.{ ExecutionContext, Future }

case class Uploaded(bucket:String, key:String)

trait S3BodyParser {

  implicit def ec: ExecutionContext

  import log.Logger

  def transferManager: TransferManager

  def uploadWithData[A](bucket: String, key: String)(predicate: RequestHeader => Either[SimpleResult, A]): BodyParser[Future[(Uploaded, A)]] = {
    Logger.debug(s"bucket=$bucket, key=$key")
    uploadWithDataMakeKey[A](bucket, _ => key)(predicate)
  }

  def uploadWithDataMakeKey[A](bucket: String, mkKey: A => String)(predicate: RequestHeader => Either[SimpleResult, A]): BodyParser[Future[(Uploaded, A)]] = BodyParser("s3-object") { request =>
    Logger.debug(s"bucket=$bucket, key=$mkKey")
    predicate(request) match {
      case Left(result) => {
        Logger.debug(s"Predicate failed - returning: ${result.header.status}")
        Done[Array[Byte], Either[SimpleResult, Future[(Uploaded, A)]]](Left(result))
      }
      case Right(data) => {
        val metadata = new ObjectMetadata

        request.headers.get(play.api.http.HeaderNames.CONTENT_LENGTH).map(_.toInt).foreach { l =>
          Logger.debug(s"contentLength=${l.toString}")
          metadata.setContentLength(l)
        }

        val output = new PipedOutputStream
        val input = new PipedInputStream(output)

        val p = scala.concurrent.promise[(Uploaded, A)]
        val f = p.future
        val key = mkKey(data)
        val upload = transferManager.upload(bucket, key, input, metadata)

        upload.addProgressListener(new ProgressListener {
          override def progressChanged(event: ProgressEvent): Unit = {
            event.getEventType match {
              case ProgressEventType.TRANSFER_COMPLETED_EVENT =>
                Logger.trace(s"bucket=$bucket, key=$key, eventType=${event.getEventType}")
               p.success{ (Uploaded(bucket, key), data) }
              case ProgressEventType.TRANSFER_FAILED_EVENT =>
                Logger.error(s"bucket=$bucket, key=$key, eventType=${event.getEventType}")
                p.failure(new RuntimeException(s"key=$key - transfer failed"))
              case ProgressEventType.TRANSFER_CANCELED_EVENT =>
                Logger.error(s"bucket=$bucket, key=$key, eventType=${event.getEventType}")
                p.failure(new RuntimeException(s"key=$key - transfer cancelled"))
              case _ => // do nothing
            }
          }
        })

        transfer(output)(request).map(_ => {
          Right(f)
        })

      }
    }
  }

  private def transfer(to: OutputStream): BodyParser[OutputStream] = BodyParser("transfer") { request =>
    Iteratee.fold[Array[Byte], OutputStream](to) { (os, data) =>
      Logger.trace(s"write bytes...")
      os.write(data)
      os
    }.map { os =>
      Logger.trace(s"done")
      os.close()
      Right(to)
    }
  }

}