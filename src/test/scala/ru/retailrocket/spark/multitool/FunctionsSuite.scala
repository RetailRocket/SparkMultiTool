
package ru.retailrocket.spark.multitool

import org.scalatest.{FunSuite,BeforeAndAfterAll}
import java.nio.file.{FileAlreadyExistsException}

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

  test("flat transform") {
    val src = sc.parallelize(List(1,2,4,0))

    val dst1 = src.flatTransform{x => Try{8/x}.toOption}
    assert(dst1.output.count() === 3)
    assert(dst1.error.count() === 0)
    assert(dst1.ignore.count() === 0)

    val dst2 = src.flatTransform{x => Seq(x,x)}
    assert(dst2.output.count() === 8)
    assert(dst2.error.count() === 0)
    assert(dst2.ignore.count() === 0)
  }

  test("save via temp and archive") {
    val root = fs.createTempDirectoryLocal("model_test")
    val data = sc.parallelize(Seq(1,2,3))
    val temp = s"${root}/model_test_temp"
    val output = s"${root}/model_test_data"

    fs.delete(output)
    data.saveViaTempWithRename(output, tempPath=Option(temp))
    assert(fs.exists(output))

    intercept[FileAlreadyExistsException] { data.saveViaTempWithRename(output, tempPath=Option(temp)) }
    Thread.sleep(1000)

    data.saveViaTempWithReplace(output, tempPath=Option(temp))
    assert(fs.exists(output))

    fs.delete(output)
    data.saveViaTempWithReplace(output, tempPath=Option(temp))
    assert(fs.exists(output))
    fs.delete(output)
  }

  override def afterAll() {
    sc.stop()
  }
}
