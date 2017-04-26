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

import sbtprotobuf.{ProtobufPlugin => PB}

val algebirdVersion = "0.12.1"
val avroVersion = "1.8.1"
val gcsVersion = "1.5.2-hadoop2"
val hadoopVersion = "2.7.3"
val jodaTimeVersion = "2.9.9"
val parquetVersion = "1.8.1"
val protoBufVersion = "2.6.1"
val scalaCheckVersion = "1.13.3"
val scalaTestVersion = "3.0.0"
val scioVersion = "0.3.0"
val scoptVersion = "3.5.0"
val slf4jVersion = "1.7.25"

val commonSettings = Seq(
  organization := "com.spotify",
  name := "ratatool",
  description := "A tool for random data sampling and generation",
  scalaVersion := "2.11.11",
  scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked")
)

val releaseSettings = Seq(
  releaseCrossBuild             := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle             := true,
  publishArtifact in Test       := false,
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

val assemblySettings = Seq(
  mainClass in assembly := Some("com.spotify.ratatool.tool.Tool"),
  assemblyMergeStrategy in assembly ~= { old => {
    case s if s.endsWith(".class") => MergeStrategy.last
    case s if s.endsWith(".dtd") => MergeStrategy.rename
    case s if s.endsWith(".properties") => MergeStrategy.filterDistinctLines
    case s if s.endsWith(".xsd") => MergeStrategy.rename
    case s => old(s)
  }},
  assemblyJarName in assembly := s"ratatool-${version.value}.jar"
)

lazy val ratatool = project
  .settings(commonSettings)
  .settings(releaseSettings)
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
  .settings(sbtavro.SbtAvro.avroSettings)
  .settings(PB.protobufSettings)
  .settings(
    version in PB.protobufConfig := protoBufVersion,
    PB.runProtoc in PB.protobufConfig := (args =>
      com.github.os72.protocjar.Protoc.runProtoc("-v261" +: args.toArray)
    )
  )
  .settings(packAutoSettings)

lazy val ratatoolScalacheck = project.in(file("ratatool-scalacheck"))
  .settings(commonSettings)
  .settings(releaseSettings)
  .settings(
    name := "ratatool-scalacheck",
    crossScalaVersions := Seq("2.11.11", "2.12.2"),
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % avroVersion,
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion
    )
  ).dependsOn(ratatool % "test")

val root = project.in(file(".")).aggregate(ratatool, ratatoolScalacheck)
