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
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.conf.Configuration


object Loaders {
  class CombineTextFileInputFormat extends CombineFileInputFormat[LongWritable, Text] {
    override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[LongWritable, Text] =
      new CombineFileRecordReader(split.asInstanceOf[CombineFileSplit], context, classOf[CombineTextFileRecordReader]);
  }

  class CombineTextFileRecordReader(split: CombineFileSplit, context: TaskAttemptContext, index: java.lang.Integer)
      extends RecordReader[LongWritable, Text] {

    val conf = context.getConfiguration
    val path = split.getPath(index)
    val fs = path.getFileSystem(conf)
    val codec = Option(new CompressionCodecFactory(conf).getCodec(path))

    val startOffset = split.getOffset(index)
    val end = Long.MaxValue

    val fileIn = codec match {
      case Some(codec) => codec.createInputStream(fs.open(path))
      case None => fs.open(path)
    }

    var reader = new LineReader(fileIn)
    var pos = startOffset;

    protected val key = startOffset
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
    override def getProgress: Float = if (startOffset == end) 0.0f else math.min(1.0f, (pos - startOffset).toFloat / (end - startOffset))
  }

  def combineTextFile(sc: SparkContext, path: String, size: Long = 256, delim: String = "\n") : RDD[String] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("textinputformat.record.delimiter", delim)
    hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    hadoopConf.set("mapred.input.dir", path)
    hadoopConf.setLong("mapred.max.split.size", size*1024*1024)

    sc.newAPIHadoopRDD(hadoopConf, classOf[CombineTextFileInputFormat], classOf[LongWritable], classOf[Text]).map(_._2.toString)
  }
}
