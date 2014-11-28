package ru.retailrocket.spark.multitool

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.util.LineReader
import org.apache.hadoop.io._
import org.apache.hadoop.fs._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured

import scala.reflect.ClassTag


object Loaders {
  abstract class Filter extends Configured with PathFilter {
    import Filter._

    private[this] var filter: Option[Traversable[Rule]] = None
    
    def check(rules: Traversable[Rule], path: Array[String]): Boolean
      
    override def accept(path: Path): Boolean = filter
      .map{f => check(f, path.toString.split(Path.SEPARATOR))}
      .getOrElse(true)

    override def setConf(conf: Configuration) {
      filter = Option(conf)
        .map(_.get(RulesPropName))
        .map(parseRules)
    }
  }

  object Filter {
    type Rule = (String, Array[String])

    val Pattern = """(\w+)=([\w|\,]+)""".r
    val RulesPropName = "ru.retailrocket.loaders.filter.rules"

    def storeRules(src: Traversable[Rule]) = src.map{
        case(k, eqs) => "%s=%s".format(k, eqs.mkString(","))
      }.mkString("&")

    def parseRules(src: String) = src.split("&").map{
        case Pattern(k, eqs) => (k, eqs.split(","))
      }
  }

  private class CombineTextFileInputFormat extends CombineFileInputFormat[LongWritable, Text] {
    override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[LongWritable, Text] =
      new CombineFileRecordReader(split.asInstanceOf[CombineFileSplit], context, classOf[CombineTextFileRecordReader])
  }

  private class CombineTextFileRecordReader(split: CombineFileSplit, context: TaskAttemptContext, index: Integer)
      extends RecordReader[LongWritable, Text] {

    val conf = context.getConfiguration
    val path = split.getPath(index)
    val fs = path.getFileSystem(conf)
    val codec = Option(new CompressionCodecFactory(conf).getCodec(path))

    val start = split.getOffset(index)
    val length = if(codec.isEmpty) split.getLength(index) else Long.MaxValue
    val end = start + length

    val fd = fs.open(path)
    if(start > 0) fd.seek(start)

    val fileIn = codec match {
      case Some(codec) => codec.createInputStream(fd)
      case None => fd
    }

    var reader = new LineReader(fileIn)
    var pos = start

    protected val key = start
    protected val value = new Text

    override def initialize(split: InputSplit, ctx: TaskAttemptContext) {}

    override def nextKeyValue(): Boolean = {
      if (pos < end) {
        val newSize = reader.readLine(value)
        pos += newSize
        newSize != 0
      } else {
        false
      }
    }
    
    override def close(): Unit = if (reader != null) { reader.close(); reader = null }
    override def getCurrentKey: LongWritable = key
    override def getCurrentValue: Text = value
    override def getProgress: Float = if (start == end) 0.0f else math.min(1.0f, (pos - start).toFloat / (end - start))
  }

  private val defaultCombineSize = 256
  private val defaultCombineDelim = "\n"

  class Context(val sc: SparkContext, val path: String) {

    val conf = new Configuration()
    conf.set("textinputformat.record.delimiter", defaultCombineDelim)
    conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    conf.set("mapred.input.dir", path)
    conf.setLong("mapred.max.split.size", defaultCombineSize*1024*1024)

    def addFilterClass[T <: Filter](filterClass: Class[T]): Context = {
      conf.set("mapreduce.input.pathFilter.class", filterClass.getName)
      this
    }

    def addFilterRules(filterRules: String) = {
      conf.set(Filter.RulesPropName, filterRules)
      this
    }

    def addFilterRules(filterRules: Traversable[Filter.Rule]) = {
      conf.set(Filter.RulesPropName, Filter.storeRules(filterRules))
      this
    }

    def addFilter[T <: Filter](filter: (Class[T], Traversable[Filter.Rule])) = this
      .addFilterClass(filter._1)
      .addFilterRules(filter._2)

    def setSplitSize(size: Long) = {
      conf.setLong("mapred.max.split.size", size*1024*1024)
      this
    }

    def setRecordDelim(delim: String) = {
      conf.set("textinputformat.record.delimiter", delim)
      this
    }

    def combine(): RDD[String] = {
      sc.newAPIHadoopRDD(conf, classOf[CombineTextFileInputFormat], classOf[LongWritable], classOf[Text]).map(_._2.toString)
    }
  }

  def forPath(sc: SparkContext, path: String) = {
    new Context(sc, path)
  }

  def combineTextFile(sc: SparkContext, path: String,
    size: Long = defaultCombineSize, delim: String = defaultCombineDelim,
    filterClass: Option[String] = None, filterRules: Option[String] = None) : RDD[String] = {

    val hadoopConf = new Configuration()
    hadoopConf.set("textinputformat.record.delimiter", delim)
    hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    hadoopConf.set("mapred.input.dir", path)
    hadoopConf.setLong("mapred.max.split.size", size*1024*1024)

    if(filterClass.isDefined && filterRules.isDefined) {
      hadoopConf.set("mapreduce.input.pathFilter.class", filterClass.get)
      hadoopConf.set(Filter.RulesPropName, filterRules.get)
    }

    sc.newAPIHadoopRDD(hadoopConf, classOf[CombineTextFileInputFormat], classOf[LongWritable], classOf[Text]).map(_._2.toString)
  }

  def combineTextFile[T:ClassTag](loader: String => T)(sc: SparkContext, path: String,
    size: Long = defaultCombineSize, delim: String = defaultCombineDelim,
    filterClass: Option[String] = None, filterRules: Option[String] = None): RDD[T] = {

    combineTextFile(sc, path, size=size, delim=delim, filterClass=filterClass, filterRules=filterRules).flatMap{s => scala.util.Try{ loader(s) }.toOption}
  }

  implicit class SparkContextFunctions(val self: SparkContext) extends AnyVal {
    def combineTextFile(path: String, size: Long = defaultCombineSize, delim: String = defaultCombineDelim): RDD[String] = Loaders.combineTextFile(self, path, size=size, delim=delim)
    def combineTextFile[T:ClassTag](loader: String => T)(path: String, size: Long = defaultCombineSize, delim: String = defaultCombineDelim): RDD[T] = Loaders.combineTextFile(loader)(self, path, size=size, delim=delim)

    def forPath(path: String): Loaders.Context = Loaders.forPath(self, path)
  }
}
