package ru.retailrocket.spark.multitool

import org.scalatest.{FunSuite,BeforeAndAfterAll}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.util._

import Implicits._


object Helpers {
  def f(x:Int): Int = 8 / x
}

class FunctionsSuite extends FunSuite with BeforeAndAfterAll {
  lazy val sc: SparkContext = new SparkContext("local", getClass.getSimpleName)
  implicit val parallel = 5

  test("transform func") {
    val src = sc.parallelize(List(1,2,4,0))
    val dst = src.transform(Helpers.f _)
    assert(dst.output.count() === 3)
    assert(dst.error.count() === 1)
    assert(dst.ignore.count() === 1)
  }

  test("transform partial") {
    val src = sc.parallelize(List(1,2,4,0))
    val dst = src.transform{case x => 8 / x}
    assert(dst.output.count() === 3)
    assert(dst.error.count() === 1)
    assert(dst.ignore.count() === 1)
  }

  test("transform with option") {
    val src = sc.parallelize(List(1,2,4,0))
    val dst = src.transform{case x => Try{8/x}.toOption}
    assert(dst.output.flatMap{x=>x}.count() === 3)
    assert(dst.error.count() === 0)
    assert(dst.ignore.count() === 0)
  }

  override def afterAll() {
    sc.stop()
  }
}
