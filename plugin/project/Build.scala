import sbt._
import sbt.Keys._

object Build extends sbt.Build {

  val playVersion = "2.1.3-RC1"
  val ScalaVersion ="2.10.1"
  val libName = "play-s3"
  val libOrganization = "org.corespring"
  val baseVersion = "0.1"

  lazy val libVersion = {
    val other = Process("git rev-parse --short HEAD").lines.head
    baseVersion + "-" + other
  }

  object Dependencies {
    val play = "play" %% "play" % playVersion % "provided"
    val playTest = "play" %% "play-test" % playVersion % "test"
    val aws = "com.amazonaws" % "aws-java-sdk" % "1.4.3"
    val config = "com.typesafe" % "config" % "1.0.0"
    val akkaTest = "com.typesafe.akka" %% "akka-testkit" % "2.1.0" % "test"
    val scalaTest = "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test"
    val all = Seq(scalaTest, play, playTest, aws, config, akkaTest)
  }

  object Resolvers {
    val typesafe = "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
    val typesafeSnapshots = "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"
    val sonatypeSnapshots = "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
    val sonatype = "sonatype" at "http://oss.sonatype.org/content/repositories/releases"
    val all = Seq(typesafe, typesafeSnapshots, sonatype, sonatypeSnapshots )
  }

  lazy val s3PlayPlugin = Project(
    id = libName ,
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      parallelExecution in(Test) := false,
      name := libName,
      organization := libOrganization,
      version := libVersion,
      scalaVersion := ScalaVersion,
      libraryDependencies ++= Dependencies.all,
      resolvers ++= Resolvers.all
    )
  )
}
