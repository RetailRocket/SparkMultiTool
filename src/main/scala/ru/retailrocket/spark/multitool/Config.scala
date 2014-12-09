package ru.retailrocket.spark.multitool

import com.typesafe.config.{ Config => TypeSafeConfig }
import org.apache.spark.{SparkContext, SparkConf}

object Config {
  def flatConfig(config: TypeSafeConfig): Seq[(String, AnyRef)] = {
    import scala.collection.convert.WrapAsScala._

    config.entrySet().map { entry =>
      val k = entry.getKey
      val v = entry.getValue.unwrapped()
      (k, v)
    }.toSeq
  }

  def asSparkConfig(config: TypeSafeConfig): SparkConf = {
    val sc = new SparkConf()

    for ((key, value) <- flatConfig(config)) {
      sc.set(key, value.toString)
    }

    sc
  }
}