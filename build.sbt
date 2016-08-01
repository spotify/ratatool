organization := "com.spotify"
name := "ratatool"
description := "A tool for random data sampling and generation"

scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.10.6", "2.11.8")
scalacOptions ++= Seq("-target:jvm-1.7", "-deprecation", "-feature", "-unchecked")
javacOptions ++= Seq("-source", "1.7", "-target", "1.7", "-Xlint:unchecked")

val avroVersion = "1.7.7"
val bijectionVersion = "0.9.0"
val dataflowVersion = "1.6.0"
val gcsVersion = "1.4.5-hadoop2"
val hadoopVersion = "2.7.2"
val jodaTimeVersion = "2.9.4"
val scalaCheckVersion = "1.13.2"
val scoptVersion = "3.5.0"
val slf4jVersion = "1.7.21"

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % scoptVersion,
  "com.google.cloud.bigdataoss" % "gcs-connector" % gcsVersion,
  "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % dataflowVersion,
  "com.twitter" %% "bijection-avro" % bijectionVersion,
  "joda-time" % "joda-time" % jodaTimeVersion,
  "org.apache.avro" % "avro-mapred" % avroVersion classifier("hadoop2"),
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
  "org.slf4j" % "slf4j-simple" % slf4jVersion
)

mainClass in assembly := Some("com.spotify.ratatool.Tool")
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case s if s.endsWith(".class") => MergeStrategy.last
  case s if s.endsWith(".dtd") => MergeStrategy.rename
  case s if s.endsWith(".properties") => MergeStrategy.filterDistinctLines
  case s if s.endsWith(".xsd") => MergeStrategy.rename
  case s => old(s)
}
}
