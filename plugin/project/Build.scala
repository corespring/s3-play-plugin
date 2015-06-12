import sbt._
import sbt.Keys._
import sbtrelease.ReleasePlugin._

object Build extends sbt.Build {

  val ScalaVersion ="2.10.3"
  val libName = "s3-play-plugin"
  val libOrganization = "org.corespring"

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
    settings = Project.defaultSettings ++ releaseSettings ++ Seq(
      parallelExecution in(Test) := false,
      name := libName,
      organization := libOrganization,
      scalaVersion := ScalaVersion,
      libraryDependencies ++= Dependencies.all,
      resolvers ++= Resolvers.all,
      credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
      publishTo <<= version {
        (v: String) =>
          def isSnapshot = v.trim.contains("-")
          val base = "http://repository.corespring.org/artifactory"
          val repoType = if (isSnapshot) "snapshot" else "release"
          val finalPath = base + "/ivy-" + repoType + "s"
          Some( "Artifactory Realm" at finalPath )
      }
    )
  )
}
