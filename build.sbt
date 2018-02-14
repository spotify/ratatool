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
val gcsVersion = "1.6.1-hadoop2"
val hadoopVersion = "2.7.3"
val jodaTimeVersion = "2.9.9"
val parquetVersion = "1.9.0"
val protoBufVersion = "3.3.1"
val scalaCheckVersion = "1.13.5"
val scalaTestVersion = "3.0.4"
val scioVersion = "0.4.3"
val scoptVersion = "3.5.0"
val slf4jVersion = "1.7.25"
val bigqueryVersion = "v2-rev372-1.23.0"

val commonSettings = Sonatype.sonatypeSettings ++ assemblySettings ++ releaseSettings ++ Seq(
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
  pomExtra                      := {
    <url>https://github.com/spotify/ratatool</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <url>git@github.com/spotify/ratatool.git</url>
      <connection>scm:git:git@github.com:spotify/ratatool.git</connection>
    </scm>
    <developers>
      <developer>
        <id>sinisa_lyh</id>
        <name>Neville Li</name>
        <url>https://twitter.com/sinisa_lyh</url>
      </developer>
    </developers>
  }
)

lazy val assemblySettings = Seq(
  mainClass in assembly := Some("com.spotify.ratatool.tool.Tool"),
  assemblyMergeStrategy in assembly ~= { old => {
    case s if s.endsWith(".class") => MergeStrategy.last
    case s if s.endsWith(".proto") => MergeStrategy.last
    case s if s.endsWith(".dtd") => MergeStrategy.rename
    case s if s.endsWith(".properties") => MergeStrategy.filterDistinctLines
    case s if s.endsWith(".xsd") => MergeStrategy.rename
    case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
    case s => old(s)
  }},
  assemblyJarName in assembly := s"ratatool-${version.value}.jar"
)

lazy val ratatool = project
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % scoptVersion,
      "com.google.cloud.bigdataoss" % "gcs-connector" % gcsVersion,
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % "test",
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.avro" % "avro" % avroVersion classifier("tests"),
      "org.apache.avro" % "avro-mapred" % avroVersion classifier("hadoop2"),
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion exclude ("org.slf4j", "slf4j-log4j12"),
      "org.apache.parquet" % "parquet-avro" % parquetVersion,
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.slf4j" % "slf4j-simple" % slf4jVersion
    ),
    // In case of scalacheck failures print more info
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "3")
  )
  .enablePlugins(ProtobufPlugin, PackPlugin)
  .settings(
    version in ProtobufConfig := protoBufVersion,
    protobufRunProtoc in ProtobufConfig := (args =>
      com.github.os72.protocjar.Protoc.runProtoc("-v330" +: args.toArray)
    )
  )

lazy val ratatoolScalacheck = project.in(file("ratatool-scalacheck"))
  .settings(commonSettings)
  .settings(
    name := "ratatool-scalacheck",
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % avroVersion,
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
      "com.google.apis" % "google-api-services-bigquery" % bigqueryVersion
    )
  ).dependsOn(ratatool % "test")
  .enablePlugins(ProtobufPlugin, PackPlugin)
  .settings(
    version in ProtobufConfig := protoBufVersion,
    protobufRunProtoc in ProtobufConfig := (args =>
      com.github.os72.protocjar.Protoc.runProtoc("-v330" +: args.toArray)
      )
  )

val root = project.in(file("."))
  .settings(commonSettings ++ noPublishSettings)
  .aggregate(ratatool, ratatoolScalacheck)
