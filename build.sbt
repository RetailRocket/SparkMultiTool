organization := "ru.retailrocket.spark"

name := "multitool"

version := "0.11"

scalaVersion := "2.11.8"

parallelExecution in Test := false

fork in Test := true

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

pomExtra :=
  <build>
    <sourceDirectory>src/main/scala</sourceDirectory>
    <plugins>
      <plugin>
        <groupId>org.scala-tools</groupId>
        <artifactId>maven-scala-plugin</artifactId>
        <version>2.15.2</version>
        <executions>
          <execution>
            <goals>
              <goal>compile</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
