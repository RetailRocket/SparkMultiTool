import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import ru.retailrocket.spark.multitool.Loaders

object MyProject {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "MyProject")
    val sessions = Loaders.combineTextFile(sc, "file://" + getClass.getResource("src/main/resource").getFile)
    println(sessions.count)
  }
}
