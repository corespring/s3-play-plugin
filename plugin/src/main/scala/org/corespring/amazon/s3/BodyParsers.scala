package org.corespring.amazon.s3

import java.io.{OutputStream, PipedInputStream, PipedOutputStream}

import com.amazonaws.event.{ProgressEvent, ProgressEventType, ProgressListener}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectMetadata, S3Object}
import com.amazonaws.services.s3.transfer.TransferManager
import play.api.libs.iteratee.{Done, Iteratee}
import play.api.mvc.{BodyParser, RequestHeader, SimpleResult}

import scala.concurrent.{ExecutionContext, Future}

trait S3BodyParser {

  implicit def ec : ExecutionContext

  import log.Logger

  def client : AmazonS3Client

  def s3Object(bucket: String, key: String)(predicate: RequestHeader=>Option[SimpleResult]) : BodyParser[Future[S3Object]] = BodyParser("s3-object") { request =>
      Logger.debug(s"bucket=$bucket, key=$key")
      predicate(request).map { err =>
        Logger.debug(s"Predicate failed - returning: ${err.header.status}")
        Done[Array[Byte], Either[SimpleResult,Future[S3Object]]](Left(err))
      }.getOrElse{
        lazy val transferManager = new TransferManager(client)

        val metadata = new ObjectMetadata

        request.headers.get(play.api.http.HeaderNames.CONTENT_LENGTH).map(_.toInt).foreach { l =>
          Logger.debug(s"contentLength=${l.toString}")
          metadata.setContentLength(l)
        }

        val output = new PipedOutputStream
        val input = new PipedInputStream(output)

        val p = scala.concurrent.promise[S3Object]
        val f = p.future
        val upload = transferManager.upload(bucket, key, input, metadata)

        upload.addProgressListener(new ProgressListener {
          override def progressChanged(event: ProgressEvent): Unit = {
            if(event.getEventType == ProgressEventType.TRANSFER_COMPLETED_EVENT){
              Logger.trace(s"bucket=$bucket, key=$key, eventType=${event.getEventType}")
              p.success{
                transferManager.shutdownNow(false)
                client.getObject(bucket, key)
              }
            }
          }
        })

        transfer(output)(request).map(_ => {
          Right(f)
        })
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