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
cd sparkmultitool
sbt package
```

#Usage

##Loaders
