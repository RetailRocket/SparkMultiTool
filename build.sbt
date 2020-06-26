organization := "ru.retailrocket.spark"

name := "multitool"

version := "0.12"

scalaVersion := "2.11.8"

parallelExecution in Test := false

fork in Test := true

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

libraryDependencies += "io.sensesecure" % "hadoop-xz" % "1.4"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

