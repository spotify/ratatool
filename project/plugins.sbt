addSbtPlugin("com.cavorite" % "sbt-avro" % "0.3.2")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.2")
addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.5.3")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.2")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
addSbtPlugin("org.scalastyle" % "scalastyle-sbt-plugin" % "0.8.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.5")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")

libraryDependencies ++= Seq(
  "com.github.os72" % "protoc-jar" % "3.0.0"
)
