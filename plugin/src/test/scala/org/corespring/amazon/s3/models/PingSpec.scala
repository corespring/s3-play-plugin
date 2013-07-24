package org.corespring.amazon.s3.models

import akka.actor.{Props, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.specs2.matcher.MustMatchers

class PingSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpec
with MustMatchers
with BeforeAndAfterAll {

  def this() = this(ActorSystem("PingSpec"))
  override def afterAll {
    system.shutdown()
  }

  "An S3Writer actor" must {

    "ping" in {

      val ref = system.actorOf(Props(new Ping()))

      ref ! PingBegin
      ref ! PingComplete

      import scala.concurrent.duration._

      expectMsgAllOf(2.seconds, PingMessage("Begin"), PingMessage("Completed"))
    }
  }

}
