addSbtPlugin("com.cavorite" % "sbt-avro-1-8" % "1.1.9")
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.14")
addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.6.5")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.1")
addSbtPlugin("org.scalastyle" % "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.9.3")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.19")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.34")

libraryDependencies ++= Seq(
  "com.github.os72" % "protoc-jar" % "3.11.4"
)

libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.8.2",
  "org.apache.avro" % "avro-compiler" % "1.8.2"
)
