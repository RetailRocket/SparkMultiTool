package ru.retailrocket.spark.multitool

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.sql._

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


object DataFrameFunctions {
  def transform[R:ClassTag](f: Row=>Option[R])(src: DataFrame): RDDFunctions.TransformResult[Row,R] = {
    val dst = src.map{s => (s, Try{f(s)})}
    val output = dst.flatMap{case (_, Success(d)) => d; case _ => None}
    val error = dst.flatMap{case (_, Failure(t)) => Some(t); case _ => None}
    val ignore = dst.flatMap{case (s, Failure(_)) => Some(s); case _ => None}
    RDDFunctions.TransformResult(output, error, ignore)
  }

  def transform[R:ClassTag](f: Row=>R)(src: DataFrame)(implicit d: DummyImplicit): RDDFunctions.TransformResult[Row,R] = {
    val dst = src.map{s => (s, Try{f(s)})}
    val output = dst.flatMap{case (_, Success(d)) => Some(d); case _ => None}
    val error = dst.flatMap{case (_, Failure(t)) => Some(t); case _ => None}
    val ignore = dst.flatMap{case (s, Failure(_)) => Some(s); case _ => None}
    RDDFunctions.TransformResult(output, error, ignore)
  }
}
