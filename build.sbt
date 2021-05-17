organizationName := "services.scalable"
name := "scheduler"

version := "main"

scalaVersion := "2.13.5"

val jacksonVersion = "2.11.4"
lazy val akkaVersion = "2.6.14"
lazy val akkaHttpVersion = "10.2.3"

libraryDependencies ++= Seq(

  "org.scalatest" %% "scalatest" % "3.2.3" % Test,

  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.apache.commons" % "commons-lang3" % "3.12.0",

  "org.apache.kafka" % "kafka-streams" % "2.8.0",

  "org.apache.kafka" % "kafka-clients" % "2.8.0",

  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,

  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,

  "com.typesafe.akka" %% "akka-http2-support" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-discovery" % akkaVersion,
  "com.typesafe.akka" %% "akka-pki" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,

  "com.typesafe.akka" %% "akka-stream-kafka" % "2.1.0",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,

  "io.vertx" % "vertx-kafka-client" % "4.0.3",

  "com.datastax.oss" % "java-driver-core" % "4.11.1",

 // "com.datastax.oss" % "java-driver-core" % "4.7.2",

)

enablePlugins(AkkaGrpcPlugin)