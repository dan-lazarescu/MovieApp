ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.3"

lazy val root = (project in file("."))
  .settings(
    name := "CsvConsumer"
  )

val akkaVersion = "2.6.15"
val akkaHttpVersion = "10.2.7"
val JacksonVersion = "2.11.4"
val scalaTestVersion = "3.3.0-SNAP3"
val kafkaVersion = "2.7.0"

resolvers ++= Seq("io.confluent" at "https://packages.confluent.io/maven/")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % "2.1.1",
  "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % "3.0.4",
//  "com.datastax.cassandra" % "cassandra-driver-core" % "4.0.0",
//  "com.datastax.cassandra" % "cassandra-driver-extras" % "3.11.0" % "optional",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "3.0.4",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.2.6",
  "com.fasterxml.jackson.core" % "jackson-databind" % JacksonVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "io.confluent" % "kafka-schema-registry-client" % "6.2.0",
  "io.confluent" % "kafka-avro-serializer" % "6.2.0",
  "org.apache.avro" % "avro" % "1.11.0",
  "org.slf4j" % "slf4j-api" % "1.7.32",
  "org.slf4j" % "slf4j-simple" % "1.7.32",
  "org.typelevel" %% "cats-core" % "2.3.0"
)