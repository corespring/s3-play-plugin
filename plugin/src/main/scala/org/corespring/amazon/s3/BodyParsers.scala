package org.corespring.amazon.s3

import java.io.{ OutputStream, PipedInputStream, PipedOutputStream }

import com.amazonaws.event.{ ProgressEvent, ProgressEventType, ProgressListener }
import com.amazonaws.services.s3.{ AmazonS3, AmazonS3Client }
import com.amazonaws.services.s3.model.{ ObjectMetadata, S3Object }
import com.amazonaws.services.s3.transfer.TransferManager
import play.api.libs.iteratee.{ Done, Iteratee }
import play.api.mvc.{ BodyParser, RequestHeader, SimpleResult }

import scala.concurrent.{ ExecutionContext, Future }

trait S3BodyParser {

  implicit def ec: ExecutionContext

  import log.Logger

  def client: AmazonS3

  def transferManager: TransferManager

  def s3ObjectAndData[A](bucket: String, key: String)(predicate: RequestHeader => Either[SimpleResult, A]): BodyParser[Future[(S3Object, A)]] = {
    Logger.debug(s"bucket=$bucket, key=$key")
    s3ObjectAndDataMakeKey[A](bucket, _ => key)(predicate)
  }

  def s3ObjectAndDataMakeKey[A](bucket: String, mkKey: A => String)(predicate: RequestHeader => Either[SimpleResult, A]): BodyParser[Future[(S3Object, A)]] = BodyParser("s3-object") { request =>
    Logger.debug(s"bucket=$bucket, key=$mkKey")
    predicate(request) match {
      case Left(result) => {
        Logger.debug(s"Predicate failed - returning: ${result.header.status}")
        Done[Array[Byte], Either[SimpleResult, Future[(S3Object, A)]]](Left(result))
      }
      case Right(data) => {
        val metadata = new ObjectMetadata

        request.headers.get(play.api.http.HeaderNames.CONTENT_LENGTH).map(_.toInt).foreach { l =>
          Logger.debug(s"contentLength=${l.toString}")
          metadata.setContentLength(l)
        }

        val output = new PipedOutputStream
        val input = new PipedInputStream(output)

        val p = scala.concurrent.promise[(S3Object, A)]
        val f = p.future
        val key = mkKey(data)
        val upload = transferManager.upload(bucket, key, input, metadata)

        upload.addProgressListener(new ProgressListener {
          override def progressChanged(event: ProgressEvent): Unit = {
            if (event.getEventType == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
              Logger.trace(s"bucket=$bucket, key=$key, eventType=${event.getEventType}, bytes=${event.getBytesTransferred}")
              p.success {
                (client.getObject(bucket, key), data)
              }
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