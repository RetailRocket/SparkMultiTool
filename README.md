SparkMultiTool
==============

Tools for spark which we use on the daily basis.
It contains:
* Loader of HDFS files with combining small files (uses Hadoop CombineFileInputFormat)
* Future: cosine calculation
* Future: Quantile calculation

#Requirements
This tool was succeffully tested with CDH 5.1.2 and Spark 1.1.0.

#Build

For building install sbt, launch a terminal, change current to sparkmultitool directory  and launch a command:

```
cd sparkmultitool
sbt package
```

#Usage

##Loaders
