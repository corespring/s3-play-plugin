import sbt._

object Dependencies {

  val playVersion = "2.2.1"

  val akkaTest = "com.typesafe.akka" %% "akka-testkit" % "2.1.0" % "test"
  val aws = "com.amazonaws" % "aws-java-sdk" % "1.10.0"
  val config = "com.typesafe" % "config" % "1.0.0"
  val play = "com.typesafe.play" %% "play" % playVersion % "provided"
  val playTest = "com.typesafe.play" %% "play-test" % playVersion % "test"
  val scalaTest = "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test"

  val all = Seq(
    akkaTest,
    aws,
    config,
    play,
    playTest,
    scalaTest
  )
}