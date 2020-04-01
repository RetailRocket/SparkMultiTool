package ru.retailrocket.spark.multitool

import org.scalatest._
import java.nio.file.FileAlreadyExistsException

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.util._

import Implicits._


object Helpers {
  def f(x:Int): Int = 8 / x

  val serializer = new StringSerializer[Int] {
      override def apply(src: Int) = s"i: ${src.toString}"
    }
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
    data.saveViaTempWithRename(Helpers.serializer)(output, tempPath=Option(temp))
    assert(fs.exists(output))

    val dst = sc.textFile(output).collect().toSet
    assert(dst === Set("i: 1", "i: 2", "i: 3"))

    intercept[FileAlreadyExistsException] { data.saveViaTempWithRename(Helpers.serializer)(output, tempPath=Option(temp)) }
    Thread.sleep(1000)

    data.saveViaTempWithReplace(Helpers.serializer)(output, tempPath=Option(temp))
    assert(fs.exists(output))

    fs.delete(output)
    data.saveViaTempWithReplace(Helpers.serializer)(output, tempPath=Option(temp))
    assert(fs.exists(output))
    fs.delete(output)
  }

  test("functions") {
    import Functions._

    {
      val seq = Seq(1->2,4->3,2->4,4->1)
      val max = seq.maxBy { _._2 }
      val min = seq.minBy { _._2 }
      assert(max === 2->4)
      assert(min === 4->1)
    }

    {
      val src = Seq(1->2, 2->3).reduce(sumTuple2[Int] _)
      assert(src === 3->5)
    }

    {
      val src = Seq(1.0->2, 2.0->3).reduce(sumTuple2[Double, Int] _)
      assert(src === 3.0->5)
    }

    {
      val src = Seq((1,2,3), (3,4,5)).reduce(sumTuple3[Int] _)
      assert(src === (4,6,8))
    }

    {
      val src = Seq((1,2.0,3L), (3,4.0,5L)).reduce(sumTuple3[Int, Double, Long] _)
      assert(src === (4,6.0,8L))
    }

    {
      assert(Seq(1,2,3).contains(1) === true)
      assert(Seq(1,2,3).contains(22) === false)
      assert(Seq(1,2,3).contains("s") === false)

      assert(Seq(1,2,3).has(1) === true)
      assert(Seq(1,2,3).has(22) === false)

      assert(Array(1,2,3).contains(1) === true)
      assert(Array(1,2,3).contains(22) === false)
      assert(Array(1,2,3).contains("s") === false)

      assert(Array(1,2,3).has(1) === true)
      assert(Array(1,2,3).has(22) === false)
      assertTypeError("""Array(1,2,3).has("s")""")
    }

    {
      assert("qq ww ee".nthIndexOf(" ", 0) == 2)
      assert("qq ww ee rr tt".nthIndexOf(" ", 3) == 11)
      assert("qq ww ee rr tt".nthSplit(" ", 3) == ("qq ww ee rr", "tt"))
    }

    {
      import Implicits.Ops
      assert("1" === "1")
      assert("1" !== "2")
    }
  }

  override def afterAll() {
    sc.stop()
  }
}
