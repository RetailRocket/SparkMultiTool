import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import ru.retailrocket.spark.multitool.Loaders._

object MyProject {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "MyProject")
    val sessions = sc.combineTextFile("file://" + getClass.getResource("src/main/resource").getFile)
    println(s"sessions count ${sessions.count()}")
  }
}
