import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName = "example"
  val appVersion = "1.0-SNAPSHOT"

  val appDependencies = Seq(
    "org.corespring" %% "s3-play-plugin" % "0.2-SNAPSHOT"
  )

  val main = play.Project(appName, appVersion, appDependencies).settings(
    resolvers += Resolver.url("Local Ivy Repository", url( "file://"+Path.userHome.absolutePath+"/.ivy2/local"))(Resolver.ivyStylePatterns)
  )

}
