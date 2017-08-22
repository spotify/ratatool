addSbtPlugin("com.cavorite" % "sbt-avro-1-7" % "1.1.2")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.8.0")
addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.6.2")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.6")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
addSbtPlugin("org.scalastyle" % "scalastyle-sbt-plugin" % "0.9.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")

libraryDependencies ++= Seq(
  "com.github.os72" % "protoc-jar" % "3.3.0.1"
)

// sbt-avro-1-7 uses avro 1.7, so we bump it to 1.8.1
libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.8.1",
  "org.apache.avro" % "avro-compiler" % "1.8.1"
)
