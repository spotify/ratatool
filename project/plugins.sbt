addSbtPlugin("com.cavorite" % "sbt-avro" % "0.3.2")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.8.0")
addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.5.3")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.3")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.1")
addSbtPlugin("org.scalastyle" % "scalastyle-sbt-plugin" % "0.8.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.4.0")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")

libraryDependencies ++= Seq(
  "com.github.os72" % "protoc-jar" % "3.0.0"
)
