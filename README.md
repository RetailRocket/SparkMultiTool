SparkMultiTool
==============

Tools for spark which we use on the daily basis.
It contains:
* Loader of HDFS files with combining small files (uses Hadoop CombineFileInputFormat)
* Future: cosine calculation
* Future: Quantile calculation

#Requirements
This library was succeffully tested with CDH 5.1.2 and Spark 1.1.0.
You should install SBT:
* [SBT tool](www.scala-sbt.org/download.html)


#Build
This build based on CDH 5.1.2 and Spark 1.1.0. Edit build.sbt If you have another environment.

For building install sbt, launch a terminal, change current to sparkmultitool directory  and launch a command:

```
sbt package
sbt test
```
Next copy spark-multitool*.jar from ./target/scala-2.10/...  to the lib folder of your sbt project.

#Usage
Include spark-multitool*.jar in --jars path in spark-submit like this:
```
spark-submit --master local --executor-memory 2G --class "Tst" --num-executors 1 --executor-cores 1 --jars lib/spark-multitool_2.10-0.1.jar target/scala-2.10/tst_2.10-0.1.jar

```
See examples folder.

##Loaders
**ru.retailrocket.spark.multitool.Loaders** - combine input files before mappers by means of Hadoop CombineFileInputFormat. In our case it reduced the number of mappers from 100000 to approx 3000 and made job significantly faster.
Parameters:
* **path** - path to the files (as in spark.textFile)
* **size** - size of target partition in Megabytes. Optimal value equals to a HDFS block size
* **delim** - line delimiters

This example loads files from "/test/*" and combine them in mappers.
```
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import ru.retailrocket.spark.multitool.Loaders._

object Tst{
	def main(args: Array[String]) ={
	val conf = new SparkConf().setMaster("local").setAppName("My App")
	val sc = new SparkContext("local", "My App")

	val path = "file:///test/*"
	val sessions = sc.combineTextFile(path)
  // or val sessions = sc.combineTextFile(path, size = 256, delim = "\n")
  // where size is split size in Megabytes, delim - line break string

	println(sessions.count())
   }
}

```
