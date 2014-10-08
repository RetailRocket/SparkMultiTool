package ru.retailrocket.spark.multitool

import org.scalatest._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import Loaders._


class LoadersSuite extends FunSuite with BeforeAndAfter {
  lazy val sc: SparkContext = new SparkContext("local", getClass.getSimpleName)
  def path(file: String) = getClass.getResource("/" + file).getFile

  test("combineTextFile") {
    val output = sc.combineTextFile(path("combine")).collect.sorted
    assert(output.deep == Array("1","2","3","4").deep)
  }

  test("combineTextFile with loader") {
    val output = sc.combineTextFile(_.toLong)(path("combine")).collect.sorted
    assert(output.deep == Array(1,2,3,4).deep)
  }
}
