package org.corespring.amazon.s3.models

import akka.actor.Actor
import scala.concurrent.Future

case class PingMessage(msg:String)

case object PingBegin
case object PingComplete

private[s3] class Ping extends Actor{

  import akka.pattern._
  import context.dispatcher

  def receive = {

    case PingBegin => Future{
      PingMessage("Begin")
    }.pipeTo(sender)
    case PingComplete => {
      sender ! PingMessage("Completed")
    }
  }
}
