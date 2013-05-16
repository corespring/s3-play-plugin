package controllers

import play.api._
import play.api.mvc._
import org.corespring.amazon.s3.ConcreteS3Service
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

  def upload(name:String) = Action( new ConcreteS3Service(key,secret).upload(bucket, name) ){

    request =>
      Ok("done")
  }
  
}