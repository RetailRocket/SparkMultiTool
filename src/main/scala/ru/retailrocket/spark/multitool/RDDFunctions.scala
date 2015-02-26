package ru.retailrocket.spark.multitool

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
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
  case class TransformResult[T, R](output: RDD[R], error: RDD[Throwable], ignore: RDD[T])

  def transform[T:ClassTag, R:ClassTag](f: T=>Option[R])(src: RDD[T]): TransformResult[T,R] = {
    val dst = src.map{s => (s, Try{f(s)})}
    val output = dst.flatMap{case (_, Success(d)) => d; case _ => None}
    val error = dst.flatMap{case (_, Failure(t)) => Some(t); case _ => None}
    val ignore = dst.flatMap{case (s, Failure(_)) => Some(s); case _ => None}
    TransformResult(output, error, ignore)
  }

  def transform[T:ClassTag, R:ClassTag](f: T=>R)(src: RDD[T])(implicit d: DummyImplicit): TransformResult[T,R] = {
    val dst = src.map{s => (s, Try{f(s)})}
    val output = dst.flatMap{case (_, Success(d)) => Some(d); case _ => None}
    val error = dst.flatMap{case (_, Failure(t)) => Some(t); case _ => None}
    val ignore = dst.flatMap{case (s, Failure(_)) => Some(s); case _ => None}
    TransformResult(output, error, ignore)
  }

  implicit class RDDFunctionsImplicits[T:ClassTag](val self: RDD[T]) {
    def transform[R:ClassTag](f: T=>Option[R]): TransformResult[T,R] = {
      RDDFunctions.transform(f)(self)
    }
    def transform[R:ClassTag](f: T=>R)(implicit d: DummyImplicit): TransformResult[T,R] = {
      RDDFunctions.transform(f)(self)
    }
  }

  class KeyBasedMultipleTextOutputFormat extends MultipleTextOutputFormat[Text, Text] {
    override def generateFileNameForKeyValue(key: Text, value: Text, name: String): String = {
      key.toString + "/" + name
    }
    
    override def generateActualKey(key: Text, value: Text) = null
  }

  def saveAsMultipleTextFiles[T:ClassTag](src: RDD[T], extractPath: T => String, root: String) {
    saveAsMultipleTextFiles(src, extractPath, root, None)
  }

  def saveAsMultipleTextFiles[T:ClassTag](src: RDD[T], extractPath: T => String, root: String, codec: Class[_ <: CompressionCodec]) {
    saveAsMultipleTextFiles(src, extractPath, root, Option(codec))
  }

  def saveAsMultipleTextFiles[T:ClassTag](src: RDD[T], extractPath: T => String, root: String, codec: Option[Class[_ <: CompressionCodec]]) {
    val hadoopConf = new Configuration()
    val jobConf = new JobConf(hadoopConf)

    jobConf.setOutputFormat(classOf[KeyBasedMultipleTextOutputFormat])

    if(codec.isDefined) {
      jobConf.setBoolean("mapred.output.compress", true)
      jobConf.setClass("mapred.output.compression.codec", codec.get, classOf[CompressionCodec])
    }

    FileOutputFormat.setOutputPath(jobConf, new Path(root))

    src
      .map{v => (new Text(extractPath(v)), new Text(v.toString))}
      .saveAsHadoopDataset(jobConf)
  }
}
