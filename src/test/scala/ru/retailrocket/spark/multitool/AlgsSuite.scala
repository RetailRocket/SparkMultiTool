package ru.retailrocket.spark.multitool

import org.scalatest._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._


class AlgsSuite extends FunSuite with BeforeAndAfterAll {
  lazy val sc: SparkContext = new SparkContext("local", getClass.getSimpleName)
  implicit val parallel = 5

  test("cosine") {
    val data = sc.parallelize(List[(Int, Long, Double)](
        (1, 1L, 0.5),
        (1, 2L, 0.3),
        (2, 1L, 0.6),
        (2, 2L, 0.2),
        (3, 1L, 0.5),
        (3, 3L, 0.2),
        (4, 1L, 0.1)))
    
    val res = algs.cosine(data).collect.sorted

    assert(res(0) === (1L,2L,0.8028463951575711), "((0.5*0.3)+(0.6*0.2)) / (sqrt(0.5^2+0.6^2+0.5^2+0.1^2)*sqrt(0.3^2+0.2^2))")
    assert(res(1) === (1L,3L,0.5360562674188974))
  }

  override def afterAll() {
    sc.stop()
  }
}
