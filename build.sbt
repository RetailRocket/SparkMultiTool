organization := "ru.retailrocket.spark"

name := "spark-multitool"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.3.0-cdh5.1.2" % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

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
