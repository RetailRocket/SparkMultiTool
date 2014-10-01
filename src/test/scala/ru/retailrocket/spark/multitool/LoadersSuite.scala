package ru.retailrocket.spark.multitool

import org.scalatest._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._


class LoadersSuite extends FunSuite with BeforeAndAfter {
  lazy val sc: SparkContext = new SparkContext("local", getClass.getSimpleName)

  test("combineTextFile") {
    def path(file: String) = getClass.getResource("/" + file).getFile

    val output = Loaders.combineTextFile(sc, path("combine")).collect.sorted
    assert(output.deep == Array("1","2","3","4").deep)
  }
}
