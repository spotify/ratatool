addDependencyTreePlugin
addSbtPlugin("com.github.sbt" % "sbt-avro" % "3.5.1")
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.20")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.4.0")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.3.1")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.4")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.5")

libraryDependencies ++= Seq(
  "org.apache.avro" % "avro-compiler" % "1.8.2",
  "org.typelevel" %% "scalac-options" % "0.1.8"
)
