/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import sbt._
import Keys._

val algebirdVersion = "0.13.2"
val avroVersion = "1.8.2"
val gcsVersion = "1.6.3-hadoop2"
val hadoopVersion = "2.7.3"
val jodaTimeVersion = "2.9.9"
val parquetVersion = "1.9.0"
val protoBufVersion = "3.3.1"
val scalaCheckVersion = "1.14.0"
val scalaTestVersion = "3.0.4"
val scioVersion = "0.5.2"
val scoptVersion = "3.5.0"
val slf4jVersion = "1.7.25"
val bigqueryVersion = "v2-rev372-1.23.0"
val beamVersion = "2.4.0"
val guavaVersion = "20.0"

val commonSettings = Sonatype.sonatypeSettings ++ releaseSettings ++ Seq(
  organization := "com.spotify",
  name := "ratatool",
  description := "A tool for random data sampling and generation",
  scalaVersion := "2.11.12",
  crossScalaVersions := Seq("2.11.12", "2.12.4"),
  scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
  scalacOptions in (Compile,doc) ++= {
    scalaBinaryVersion.value match {
      case "2.12" => "-no-java-comments" :: Nil
      case _ => Nil
    }
  },
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked")
)

lazy val protoBufSettings = Seq(
  version in ProtobufConfig := protoBufVersion,
  protobufRunProtoc in ProtobufConfig := (args =>
    com.github.os72.protocjar.Protoc.runProtoc("-v330" +: args.toArray)
    )
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val releaseSettings = Seq(
  releaseCrossBuild             := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle             := true,
  publishArtifact in Test       := false,
  publishTo := Some(if (isSnapshot.value) Opts.resolver.sonatypeSnapshots else Opts.resolver.sonatypeStaging),
  sonatypeProfileName           := "com.spotify",

  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/spotify/ratatool")),
  scmInfo := Some(ScmInfo(
    url("https://github.com/spotify/ratatool.git"),
    "scm:git:git@github.com:spotify/ratatool.git")),
  developers := List(
    Developer(id="sinisa_lyh", name="Neville Li", email="neville.lyh@gmail.com", url=url("https://twitter.com/sinisa_lyh")),
    Developer(id="ravwojdyla", name="Rafal Wojdyla", email="ravwojdyla@gmail.com", url=url("https://twitter.com/ravwojdyla")),
    Developer(id="idreeskhan", name="Idrees Khan", email="me@idreeskhan.com", url=url("https://github.com/idreeskhan"))
  )
)

lazy val ratatoolCommon = project
  .in(file("ratatool-common"))
  .settings(commonSettings)
  .settings(
    name := "ratatool-common",
    libraryDependencies ++= Seq(
      "com.google.cloud.bigdataoss" % "gcs-connector" % gcsVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.avro" % "avro" % avroVersion classifier("tests"),
      "org.apache.avro" % "avro-mapred" % avroVersion classifier("hadoop2"),
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion exclude ("org.slf4j", "slf4j-log4j12"),
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      "com.google.apis" % "google-api-services-bigquery" % bigqueryVersion % "provided"
    ),
    // In case of scalacheck failures print more info
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "3")
  )
  .enablePlugins(ProtobufPlugin)
  .settings(protoBufSettings)

lazy val ratatoolSampling = project
  .in(file("ratatool-sampling"))
  .settings(commonSettings)
  .settings(
    name := "ratatool-sampling",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % "test",
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.parquet" % "parquet-avro" % parquetVersion,
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    ),
    // In case of scalacheck failures print more info
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "3"),
    parallelExecution in Test := false
  )
  .enablePlugins(ProtobufPlugin)
  .dependsOn(
    ratatoolCommon % "compile->compile;test->test",
    ratatoolScalacheck % "test"
  )

lazy val ratatoolDiffy = project
  .in(file("ratatool-diffy"))
  .settings(commonSettings)
  .settings(
    name := "ratatool-diffy",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % "test",
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "joda-time" % "joda-time" % jodaTimeVersion
    ),
    // In case of scalacheck failures print more info
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "3"),
    parallelExecution in Test := false
  )
  .enablePlugins(ProtobufPlugin)
  .dependsOn(
    ratatoolCommon % "compile->compile;test->test",
    ratatoolSampling,
    ratatoolScalacheck % "test"
  )

lazy val ratatoolCli = project
  .in(file("ratatool-cli"))
  .settings(commonSettings ++ noPublishSettings)
  .settings(
    name := "ratatool-cli",
    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % scoptVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    ),
    // In case of scalacheck failures print more info
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "3")
  )
  .enablePlugins(ProtobufPlugin, PackPlugin)
  .dependsOn(
    ratatoolCommon % "compile->compile;test->test",
    ratatoolSampling,
    ratatoolDiffy
  )

lazy val ratatoolScalacheck = project
  .in(file("ratatool-scalacheck"))
  .settings(commonSettings)
  .settings(
    name := "ratatool-scalacheck",
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % avroVersion,
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
      "com.google.apis" % "google-api-services-bigquery" % bigqueryVersion % "provided",
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion
    )
  )
  .enablePlugins(ProtobufPlugin)
  .dependsOn(ratatoolCommon % "compile->compile;test->test")

lazy val ratatoolExamples = project
  .in(file("ratatool-examples"))
  .settings(commonSettings ++ noPublishSettings)
  .settings(
    name := "ratatool-examples",
    libraryDependencies ++= Seq(
      "com.google.apis" % "google-api-services-bigquery" % bigqueryVersion
    )
  )
  .enablePlugins(ProtobufPlugin, PackPlugin)
  .dependsOn(
    ratatoolCommon,
    ratatoolScalacheck
  )

val root = project.in(file("."))
  .settings(commonSettings ++ noPublishSettings)
  .aggregate(
    ratatoolCommon,
    ratatoolScalacheck,
    ratatoolDiffy,
    ratatoolSampling,
    ratatoolCli
  )
