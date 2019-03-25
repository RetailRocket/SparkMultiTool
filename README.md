SparkMultiTool
==============

Tools for spark which we use on the daily basis.
It contains:
* Loader of HDFS files with combining small files (uses Hadoop CombineTextInputFormat/CombineFileInputFormat)
* Future: cosine calculation
* Future: quantile calculation

#Requirements
This library was succeffully tested with Scala 2.11.8 and Spark 2.3.1.
You should install SBT:
* [SBT tool](www.scala-sbt.org/download.html)


#Build
This build based on Scala 2.11.8 and Spark 2.3.1. Edit build.sbt If you have another environment.

For building install sbt, launch a terminal, change current to sparkmultitool directory  and launch a command:

```
sbt package
sbt test
```
Next copy spark-multitool*.jar from ./target/scala-2.11/...  to the lib folder of your sbt project.

#Usage
Include spark-multitool*.jar in --jars path in spark-submit like this:
```
spark-submit --master local --executor-memory 2G --class "Tst" --num-executors 1 --executor-cores 1 --jars lib/spark-multitool_2.11-0.8.jar target/scala-2.11/tst_2.11-0.1.jar

```
See examples folder.

##Loaders
**ru.retailrocket.spark.multitool.Loaders** - combine input files before mappers by means of Hadoop CombineTextInputFormat/CombineFileInputFormat. In our case it reduced the number of mappers from 100000 to approx 3000 and made job significantly faster.
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

object Tst {
	def main(args: Array[String]) = {
	  val conf = new SparkConf().setMaster("local").setAppName("My App")
	  val sc = new SparkContext("local", "My App")

    val path = "file:///test/*"

    {
      val sessions = sc
        .forPath(path)
        .setSplitSize(256) // optional
        .setRecordDelim("\n") // optional
        .combine()
	    println(sessions.count())
    }

    {
      // you can also get RDD[(String, String)] with (file, line)
      val sessions = sc
        .forPath(path)
        .combineWithPath()
	    println(sessions.count())

      {
        // or add path filter, e.g. for partitioning
        class FileNameEqualityFilter extends Filter {
          def check(rules: Traversable[Filter.Rule], path: Array[String]) = {
            rules.forall {
              case(k, Array(eq)) =>
                k match {
                  case "file" => eq == path.last
                  case _ => false
                }
            }
          }
        }
        val sessions = sc
          .forPath(path)
          .addFilter(classOf[FileNameEqualityFilter], Seq("file" -> Array("file.name")))
          .combine()
	      println(sessions.count())
      }
    }
  }
}
```

##Algorithms

**ru.retailrocket.spark.multitool.algs.cosine** - cosine similarity function.

##Utility

**ru.retailrocket.spark.multitool.HashFNV** - simple, but useful hash function. Original idea from org.apache.pig.piggybank.evaluation.string.HashFNV
