package controllers

import play.api._
import play.api.mvc._
import org.corespring.amazon.s3._
import com.typesafe.config.ConfigFactory
import play.libs.Akka
import akka.actor.ActorSystem

object Application extends Controller {

  lazy val key = ConfigFactory.load().getString("amazonKey")
  lazy val secret = ConfigFactory.load().getString("amazonSecret")
  lazy val bucket = ConfigFactory.load().getString("exampleBucket")

  implicit val actorSystem : ActorSystem = Akka.system()
  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }

  val s3Service = new ConcreteS3Service(key, secret, Some("http://localhost:4567"))

  def upload(name:String) = Action( s3Service.upload(bucket, name) ){
    request =>
      Ok("done")
  }

  def download(name:String) = Action{
    request => s3Service.download(bucket, name)
  }
  
}