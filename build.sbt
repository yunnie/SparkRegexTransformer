import Dependencies._

ThisBuild / scalaVersion     := "2.12.15"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "dev.yunified"
ThisBuild / organizationName := "yunified by design"

lazy val root = (project in file("."))
  .settings(
    name := "regextransformer",
    libraryDependencies += scalaTest % Test
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.2.0" % "provided",
  "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test"
)
// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
