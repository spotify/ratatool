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

import sbt.{Def, _}
import Keys._

val algebirdVersion = "0.13.8"
val avroVersion = "1.8.2"
val beamVersion = "2.33.0"
val bigqueryVersion = "v2-rev20210410-1.31.0"
val gcsVersion = "2.1.3"
val guavaVersion = "30.1-jre" // make sure this stays compatible with scio + beam
val hadoopVersion = "2.10.1"
val jodaTimeVersion = "2.10.13"
val parquetVersion = "1.12.2"
val protoBufVersion = "3.18.1"
val scalaTestVersion = "3.2.10"
val scalaCheckVersion = "1.15.4"
val scalaCollectionCompatVersion = "2.5.0"
val scioVersion = "0.11.1"
val scoptVersion = "4.0.1"
val shapelessVersion = "2.3.7"
val sourcecodeVersion = "0.2.7"
val slf4jVersion = "1.7.32"

def isScala213x: Def.Initialize[Boolean] = Def.setting {
  scalaBinaryVersion.value == "2.13"
}

val commonSettings = Sonatype.sonatypeSettings ++ releaseSettings ++ Seq(
  organization := "com.spotify",
  name := "ratatool",
  description := "A tool for random data sampling and generation",
  scalaVersion := "2.12.10",
  crossScalaVersions := Seq("2.12.10", "2.13.6"),
  resolvers += "confluent" at "https://packages.confluent.io/maven/",
  scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked", "-Yrangepos"),
  scalacOptions in (Compile, doc) ++= {
    scalaBinaryVersion.value match {
      case "2.12" => "-no-java-comments" :: Nil
      case _      => Nil
    }
  },
  scalacOptions ++= {
    if (isScala213x.value) {
      Seq("-Ymacro-annotations", "-Ywarn-unused")
    } else {
      Seq()
    }
  },
  libraryDependencies ++= {
    if (isScala213x.value) {
      Seq()
    } else {
      Seq(
        "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion
      )
    }
  },
  sourceDirectories in Compile := (sourceDirectories in Compile).value
    .filterNot(_.getPath.endsWith("/src_managed/main")),
  managedSourceDirectories in Compile := (managedSourceDirectories in Compile).value
    .filterNot(_.getPath.endsWith("/src_managed/main")),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
  addCompilerPlugin(scalafixSemanticdb),
  run / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
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
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle := true,
  publishArtifact in Test := false,
  publishTo := Some(
    if (isSnapshot.value) Opts.resolver.sonatypeSnapshots else Opts.resolver.sonatypeStaging
  ),
  sonatypeProfileName := "com.spotify",
  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/spotify/ratatool")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/spotify/ratatool.git"),
      "scm:git:git@github.com:spotify/ratatool.git"
    )
  ),
  developers := List(
    // current maintainers
    Developer(
      id = "anne-decusatis",
      name = "Anne DeCusatis",
      email = "anned@spotify.com",
      url = url("https://twitter.com/precisememory")
    ),
    Developer(
      id = "catherinejelder",
      name = "Catherine Elder",
      email = "siege@spotify.com",
      url = url("https://twitter.com/siegeelder")
    ),
    Developer(
      id = "idreeskhan",
      name = "Idrees Khan",
      email = "me@idreeskhan.com",
      url = url("https://twitter.com/idreesxkhan")
    ),
    // past contributors
    Developer(
      id = "sinisa_lyh",
      name = "Neville Li",
      email = "neville.lyh@gmail.com",
      url = url("https://twitter.com/sinisa_lyh")
    ),
    Developer(
      id = "ravwojdyla",
      name = "Rafal Wojdyla",
      email = "ravwojdyla@gmail.com",
      url = url("https://twitter.com/ravwojdyla")
    )
  )
)

lazy val ratatoolCommon = project
  .in(file("ratatool-common"))
  .settings(commonSettings)
  .settings(
    name := "ratatool-common",
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.avro" % "avro" % avroVersion classifier "tests",
      "org.apache.avro" % "avro-mapred" % avroVersion classifier "hadoop2",
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      "com.google.apis" % "google-api-services-bigquery" % bigqueryVersion % "provided",
      "com.google.guava" % "guava" % guavaVersion
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
      "com.spotify" %% "scio-avro" % scioVersion,
      "com.spotify" %% "scio-google-cloud-platform" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % "test",
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
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
  .settings(protoBufSettings)

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
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    ),
    // In case of scalacheck failures print more info
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "3"),
    parallelExecution in Test := false,
    libraryDependencies ++= {
      if (isScala213x.value) {
        Seq()
      } else {
        Seq(
          compilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)
        )
      }
    }
  )
  .enablePlugins(ProtobufPlugin)
  .dependsOn(
    ratatoolCommon % "compile->compile;test->test",
    ratatoolSampling,
    ratatoolScalacheck % "test"
  )
  .settings(protoBufSettings)

lazy val ratatoolShapeless = project
  .in(file("ratatool-shapeless"))
  .settings(commonSettings)
  .settings(
    name := "ratatool-shapeless",
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    ),
    // In case of scalacheck failures print more info
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "3"),
    parallelExecution in Test := false
  )
  .dependsOn(
    ratatoolDiffy,
    ratatoolSampling
  )

lazy val ratatoolExtras = project
  .in(file("ratatool-extras"))
  .settings(commonSettings)
  .settings(
    name := "ratatool-extras",
    libraryDependencies ++= Seq(
      "org.apache.parquet" % "parquet-avro" % parquetVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion exclude ("org.slf4j", "slf4j-log4j12"),
      "com.google.cloud.bigdataoss" % "gcs-connector" % s"hadoop2-$gcsVersion",
      "com.google.cloud.bigdataoss" % "util" % gcsVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    ),
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "3")
  )
  .dependsOn(
    ratatoolSampling % "compile->compile;test->test",
    ratatoolCommon % "test->test",
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
  .settings(protoBufSettings)

lazy val ratatoolScalacheck = project
  .in(file("ratatool-scalacheck"))
  .settings(commonSettings)
  .settings(
    name := "ratatool-scalacheck",
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % avroVersion,
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
      "com.google.apis" % "google-api-services-bigquery" % bigqueryVersion % "provided",
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "com.lihaoyi" %% "sourcecode" % sourcecodeVersion
    )
  )
  .enablePlugins(ProtobufPlugin)
  .dependsOn(ratatoolCommon % "compile->compile;test->test")
  .settings(protoBufSettings)

lazy val ratatoolExamples = project
  .in(file("ratatool-examples"))
  .settings(commonSettings ++ noPublishSettings)
  .settings(
    name := "ratatool-examples",
    libraryDependencies ++= Seq(
      "com.google.apis" % "google-api-services-bigquery" % bigqueryVersion,
      "com.spotify" %% "scio-test" % scioVersion % "test"
    )
  )
  .enablePlugins(ProtobufPlugin)
  .dependsOn(
    ratatoolCommon,
    ratatoolScalacheck,
    ratatoolDiffy
  )
  .settings(protoBufSettings)

val root = project
  .in(file("."))
  .settings(commonSettings ++ noPublishSettings)
  .aggregate(
    ratatoolCommon,
    ratatoolScalacheck,
    ratatoolDiffy,
    ratatoolSampling,
    ratatoolShapeless,
    ratatoolExtras,
    ratatoolCli,
    ratatoolExamples
  )
