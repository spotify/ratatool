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

val algebirdVersion = "0.13.10"

// Keep in sync with Scio: https://github.com/spotify/scio/blob/v0.14.0/build.sbt
val scioVersion = "0.14.1"

val avroVersion = avroCompilerVersion // keep in sync with scio
val beamVersion = "2.53.0" // keep in sync with scio
val beamVendorVersion = "0.1" // keep in sync with scio
val bigqueryVersion = "v2-rev20230812-2.0.0" // keep in sync with scio
val floggerVersion = "0.8" // keep in sync with scio + beam
val guavaVersion = "32.1.2-jre" // keep in sync with scio + beam
val hadoopVersion = "3.2.4" // keep in sync with scio
val jodaTimeVersion = "2.10.10" // keep in sync with scio
val parquetVersion = "1.13.1" // keep in sync with scio
val protoBufVersion = "3.25.1" // keep in sync with scio
val scalaTestVersion = "3.2.18"
val scalaCheckVersion = "1.17.0"
val scalaCollectionCompatVersion = "2.11.0"
val scoptVersion = "4.1.0"
val shapelessVersion = "2.3.10" // keep in sync with scio
val sourcecodeVersion = "0.3.1"
val slf4jVersion = "1.7.30" // keep in sync with scio

def isScala213x: Def.Initialize[Boolean] = Def.setting {
  scalaBinaryVersion.value == "2.13"
}

val commonSettings = Sonatype.sonatypeSettings ++ releaseSettings ++ Seq(
  organization := "com.spotify",
  name := "ratatool",
  description := "A tool for random data sampling and generation",
  scalaVersion := "2.12.18",
  crossScalaVersions := Seq("2.12.18", "2.13.12"),
  resolvers ++= Resolver.sonatypeOssRepos("public"),
  resolvers ++= Resolver.sonatypeOssRepos("snapshots"), // @Todo remove when 0.14.0 released
  scalacOptions ++= Seq("-target:8", "-deprecation", "-feature", "-unchecked", "-Yrangepos"),
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
  excludeDependencies += "org.apache.beam" % "beam-sdks-java-io-kafka",
  javacOptions ++= Seq("--release", "8", "-Xlint:unchecked"),
  fork := true
)

ThisBuild / PB.protocVersion := protoBufVersion

lazy val protoBufSettings = Seq(
  libraryDependencies ++= Seq(
    "com.google.protobuf" % "protobuf-java" % protoBufVersion % "protobuf",
    "com.google.protobuf" % "protobuf-java" % protoBufVersion
  )
) ++ Seq(Compile, Test).flatMap(c =>
  inConfig(c)(
    Def.settings(
      PB.targets := Seq(
        PB.gens.java -> (ThisScope.copy(config = Zero) / sourceManaged).value /
          "compiled_proto" /
          Defaults.nameForSrc(configuration.value.name)
      ),
      managedSourceDirectories ++= PB.targets.value.map(_.outputPath)
    )
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
  Test / publishArtifact := false,
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
    ),
    Developer(
      id = "benk",
      name = "Ben Konz",
      email = "benkonz16@gmail.com",
      url = url("https://benkonz.github.io/")
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
      "com.google.guava" % "guava" % guavaVersion,
      "com.google.apis" % "google-api-services-bigquery" % bigqueryVersion % Test,
      "org.apache.avro" % "avro" % avroVersion % Test,
      "org.apache.avro" % "avro" % avroVersion % Test classifier "tests",
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test
    )
  )
  .settings(protoBufSettings)

lazy val ratatoolSampling = project
  .in(file("ratatool-sampling"))
  .settings(commonSettings)
  .settings(
    name := "ratatool-sampling",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-avro" % scioVersion,
      "com.spotify" %% "scio-parquet" % scioVersion,
      "com.spotify" %% "scio-google-cloud-platform" % scioVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Provided,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
      "org.scalatestplus" %% "scalacheck-1-17" % s"$scalaTestVersion.0" % Test
    ),
    Test / parallelExecution := false
  )
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
      "com.spotify" %% "scio-parquet" % scioVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "joda-time" % "joda-time" % jodaTimeVersion,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Test
    ),
    Test / parallelExecution := false,
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
  .dependsOn(
    ratatoolCommon % "compile->compile;test->test",
    ratatoolSampling % "compile->compile;test->test",
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
    Test / parallelExecution := false
  )
  .dependsOn(
    ratatoolDiffy,
    ratatoolSampling
  )

lazy val ratatoolCli = project
  .in(file("ratatool-cli"))
  .settings(commonSettings ++ noPublishSettings)
  .settings(
    name := "ratatool-cli",
    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % scoptVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test
    )
  )
  .enablePlugins(PackPlugin)
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
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
      "com.lihaoyi" %% "sourcecode" % sourcecodeVersion,
      "com.google.apis" % "google-api-services-bigquery" % bigqueryVersion % Provided,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
      "org.scalatestplus" %% "scalacheck-1-17" % s"$scalaTestVersion.0" % Test
    )
  )
  .dependsOn(ratatoolCommon % "compile->compile;test->test")
  .settings(protoBufSettings)

lazy val ratatoolExamples = project
  .in(file("ratatool-examples"))
  .settings(commonSettings ++ noPublishSettings)
  .settings(
    name := "ratatool-examples",
    libraryDependencies ++= Seq(
      "com.google.apis" % "google-api-services-bigquery" % bigqueryVersion,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "org.scalatestplus" %% "scalacheck-1-17" % s"$scalaTestVersion.0" % Test
    )
  )
  .dependsOn(
    ratatoolCommon % "compile->test",
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
    ratatoolCli,
    ratatoolExamples
  )
