SparkMultiTool
==============

Tools for spark which we use on the daily basis.
It contains:
* Loader of HDFS files with combining small files (uses Hadoop CombineFileInputFormat)
* Future: cosine calculation
* Future: Quantile calculation

#Requirements
This tool was succeffully tested with CDH 5.1.2 and Spark 1.1.0.
You should install these components:
* [SBT tool](www.scala-sbt.org/download.html)
* [Spark libraries](spark.apache.org/downloads.html)
* [Cloudera distributive](www.cloudera.com/content/support/en/downloads.html), choose link to CDH

#Build

For building install sbt, launch a terminal, change current to sparkmultitool directory  and launch a command:

```
sbt package
```
Next copy spark-multitool*.jar from ./target/scala-2.10/...  to the lib folder of your sbt project.

#Usage

##Loaders
```

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import ru.retailrocket.spark.multitool.Loaders

val conf = new SparkConf().setMaster("local").setAppName("My App")
val sc = new SparkContext("local", "My App")

val sessions = Loaders.combineTextFile(sc, conf.weblogs())
// or val sessions = Loaders.combineTextFile(sc, conf.weblogs(), size = 256, delim = "\n")
// where size is split size in Megabytes, delim - line break string

sessions.count()
```
