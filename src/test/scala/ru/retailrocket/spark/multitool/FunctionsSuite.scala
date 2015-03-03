package ru.retailrocket.spark.multitool

import org.scalatest._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import Implicits._


class FunctionsSuite extends FunSuite with BeforeAndAfterAll {
  lazy val sc: SparkContext = new SparkContext("local", getClass.getSimpleName)
  implicit val parallel = 5

  test("transform") {
    val src = sc.parallelize(List(1,2,4,0))
    val dst = src.transform{x: Int => 8 / x}
    assert(dst.output.count() === 3)
    assert(dst.error.count() === 1)
    assert(dst.ignore.count() === 1)
  }

  override def afterAll() {
    sc.stop()
  }
}
