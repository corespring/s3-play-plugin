import sbt._
import sbt.Keys._

object S3PlayPluginBuild extends Build {

  object Dependencies {
    val play = "play" %% "play" % "2.1.1" % "provided"
    val playTest = "play" %% "play-test" % "2.1.1" % "test"
    val specs2 = "org.specs2" %% "specs2" % "1.14" % "test"
    val aws = "com.amazonaws" % "aws-java-sdk" % "1.4.3"
    val config = "com.typesafe" % "config" % "1.0.0"
    val akkaTest = "com.typesafe.akka" %% "akka-testkit" % "2.1.0" % "test"
    val scalaTest = "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test"
    val all = Seq(scalaTest, specs2, play, playTest, aws, config, akkaTest)
  }

  object Resolvers {
    val typesafe = "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
    val typesafeSnapshots = "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"
    val sonatypeSnapshots = "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
    val sonatype = "sonatype" at "http://oss.sonatype.org/content/repositories/releases"
    val all = Seq(typesafe, typesafeSnapshots, sonatype, sonatypeSnapshots )
  }

  lazy val s3PlayPlugin = Project(
    id = "s3-play-plugin",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      parallelExecution in(Test) := false,
      name := "s3-play-plugin",
      organization := "corespring",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.1",
      libraryDependencies ++= Dependencies.all,
      resolvers ++= Resolvers.all
      // add other settings here
    )
  )
}
