addDependencyTreePlugin
addSbtPlugin("com.github.sbt" % "sbt-avro" % "3.4.4")
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.19")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.4.0")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.2.1")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.11")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.10.0")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.11.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

libraryDependencies ++= Seq("org.apache.avro" % "avro-compiler" % "1.8.2")
