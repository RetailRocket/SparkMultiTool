package ru.retailrocket.spark.multitool

import org.scalatest._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import Loaders._
import Loaders.Filter


class FileNameEqualityFilter extends Filter {
  def check(rules: Traversable[Filter.Rule], path: Array[String]) = {
    rules.forall{
      case(k, Array(eq)) =>
        k match {
          case "file" => eq == path.last
          case _ => false
        }
    }
  }
}

class LoadersSuite extends FunSuite with BeforeAndAfterAll {
  lazy val sc: SparkContext = new SparkContext("local", getClass.getSimpleName)
  def path(file: String) = getClass.getResource("/" + file).getFile

  test("forPathAndCombine") {
    val output = sc.forPath(path("combine")).combine().collect.sorted
    assert(output.deep == Array("1","2","3","4").deep)
  }

  test("forPathAndCombineWithPath") {
    val output = sc.forPath(path("combine")).combineWithPath().collect.sorted
    assert(output(1)._1.endsWith("file_1.csv"))
  }

  test("forPathWithFilter") {
    val output = sc.forPath(path("combine")+"/*")
      .addFilter(classOf[FileNameEqualityFilter], Seq("file" -> Array("file_2.csv")))
      .combine().collect.sorted
    assert(output.deep == Array("3","4").deep)
  }

  test("compression") {
    {
      val actual = sc.forPath(path("archive")+"/test_gzip.txt.gz").combine().collect().head
      assert(actual === "gzip")
    }

    {
      val actual = sc.forPath(path("archive")+"/test_bzip2.txt.bz2").combine().collect().head
      assert(actual === "bzip2")
    }

    {
      val actual = sc.forPath(path("archive")+"/test_lzma.txt.xz").combine().collect().head
      assert(actual === "lzma")
    }
  }

  override def afterAll() {
    sc.stop()
  }
}
