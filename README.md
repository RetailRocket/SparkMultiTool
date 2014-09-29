SparkMultiTool
==============

Tools for spark which we use on the daily basis.
It contains:
* Loader of HDFS files with combining small files (uses Hadoop CombineFileInputFormat)
* Future: cosine calculation
* Future: Quantile calculation


Build
=====
For building install sbt and lauch commands in terminal:

```
cd sparkmultitool
sbt package
```

How-To
======

Loaders
=======
