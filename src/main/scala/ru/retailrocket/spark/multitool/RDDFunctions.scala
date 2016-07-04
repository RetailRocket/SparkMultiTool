package ru.retailrocket.spark.multitool

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd._

import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.util.LineReader
import org.apache.hadoop.io._
import org.apache.hadoop.fs._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured

import scala.reflect.ClassTag
import scala.reflect._
import scala.util._


object RDDFunctions {
  val DefaultPersistLevel = StorageLevel.MEMORY_AND_DISK_SER

  case class TransformResult[T, R: ClassTag](output: RDD[R], error: RDD[Throwable], ignore: RDD[T]) {
    def name = classTag[R].toString
    def summary: String = s"${name} output ${output.count()} ignore ${ignore.count()}"
    def cache(): TransformResult[T,R] =
      TransformResult(output.cache(), error.cache(), ignore.cache())
    def persist(level: StorageLevel=DefaultPersistLevel): TransformResult[T,R] =
      TransformResult(output.persist(level), error.persist(level), ignore.persist(level))
  }

  def transform[T:ClassTag, R:ClassTag](f: T=>R)(src: RDD[T]): TransformResult[T,R] = {
    val dst = src.map{s => (s, Try{f(s)})}
    val output = dst.flatMap{case (_, Success(d)) => Some(d); case _ => None}
    val error = dst.flatMap{case (_, Failure(t)) => Some(t); case _ => None}
    val ignore = dst.flatMap{case (s, Failure(_)) => Some(s); case _ => None}
    TransformResult(output, error, ignore)
  }

  def flatTransform[T:ClassTag, R:ClassTag, C<%TraversableOnce[R]](f: T=>C)(src: RDD[T]): TransformResult[T,R] = {
    val dst = src.map{s => (s, Try{f(s)})}
    val output = dst.flatMap{case (_, Success(d)) => d; case _ => None}
    val error = dst.flatMap{case (_, Failure(t)) => Some(t); case _ => None}
    val ignore = dst.flatMap{case (s, Failure(_)) => Some(s); case _ => None}
    TransformResult(output, error, ignore)
  }

  class KeyBasedMultipleTextOutputFormat extends MultipleTextOutputFormat[Text, Text] {
    override def generateFileNameForKeyValue(key: Text, value: Text, name: String): String = {
      key.toString + "/" + name
    }
    
    override def generateActualKey(key: Text, value: Text) = null
  }

  def saveAsMultipleTextFiles[T:ClassTag](src: RDD[T], root: String)(getPath: T => String)(getData: T => _) {
    saveAsMultipleTextFiles(src, root, None)(getPath)(getData)
  }

  def saveAsMultipleTextFiles[T:ClassTag](src: RDD[T], root: String, codec: Class[_ <: CompressionCodec])(getPath: T => String)(getData: T => _) {
    saveAsMultipleTextFiles(src, root, Option(codec))(getPath)(getData)
  }

  def saveAsMultipleTextFiles[T:ClassTag](src: RDD[T], root: String, codec: Option[Class[_ <: CompressionCodec]])(getPath: T => String)(getData: T => _) {
    val hadoopConf = new Configuration()
    val jobConf = new JobConf(hadoopConf)

    jobConf.setOutputFormat(classOf[KeyBasedMultipleTextOutputFormat])

    if(codec.isDefined) {
      jobConf.setBoolean("mapred.output.compress", true)
      jobConf.setClass("mapred.output.compression.codec", codec.get, classOf[CompressionCodec])
    }

    FileOutputFormat.setOutputPath(jobConf, new Path(root))

    src
      .map { v => (new Text(getPath(v)), new Text(getData(v).toString)) }
      .saveAsHadoopDataset(jobConf)
  }
}
