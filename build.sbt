import _root_.sbt.Keys._
import sbt.Keys._
import sbt._
import Dependencies._
import xerial.sbt.Pack._

lazy val commonSettings = Seq(
  organization := "com.shashi",
  version := "0.1",
  scalaVersion := "2.10.5"
)

lazy val testSettings = Seq(
  parallelExecution in Test := false,
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
)


lazy val sensoranalytics = project.in(file(".")).aggregate(core).
  settings(packSettings: _*).
  settings(testSettings: _*).
  settings(
    name :="sensoranalytics",
    packMain := Map("core" -> "com.shashi.spark.streaming.StreamingMain"),
    packExtraClasspath := Map("core" -> Seq("${PROG_HOME}")),
    packResourceDir += (baseDirectory.value / "rest/src/main/resources/conf" -> "conf"),
    packExcludeJars := Seq("servlet-api-2.5-.*\\.jar")
  )

lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(testSettings: _*).
  settings(
    name := "core",
    libraryDependencies ++= Seq(
      Libraries.sparkCore,
      Libraries.sparkSql,
      Libraries.sparkHive,
      Libraries.sparkKafka,
      Libraries.cassandraConnector,
      Libraries.scalaTest,
      Libraries.sparkStreaming
    )
  )