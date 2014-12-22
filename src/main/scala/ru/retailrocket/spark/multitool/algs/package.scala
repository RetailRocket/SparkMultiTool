package ru.retailrocket.spark.multitool

import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

import scala.util._
import scala.reflect.ClassTag

package object algs {
  def cosine[Session : ClassTag, Item <% Ordered[Item] : ClassTag](src: RDD[(Session, Item, Double)])(implicit parallel: Int): RDD[(Item, Item, Double)] = {
    val ab = src
      .map{case(session, item, weight) => (session, (item, weight))}
      .groupByKey(parallel)
      .flatMap{case(prop, items) =>
        for((itemA, weightA) <- items; (itemB, weightB) <- items if itemA < itemB)
        yield ((itemA, itemB), weightA * weightB)}
      .reduceByKey(_+_, parallel)
      .map{case((itemA, itemB), weight) => (itemA, itemB, weight)}

    val a = src
      .map{case(session, item, weight) => (item, weight * weight)}
      .reduceByKey(_+_, parallel)
      .map{case(item, weight) => (item, math.sqrt(weight))}
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val cosineL = ab
      .map{case(itemA, itemB, weightAB) => (itemA, (itemB, weightAB))}
      .join(a, parallel)
      .map{case(itemA, ((itemB, weightAB), weightA)) => (itemB, (itemA, weightAB, weightA))}
      .join(a, parallel)
      .map{case(itemB, ((itemA, weightAB, weightA), weightB)) => (itemA, itemB, weightAB / (weightA * weightB))}

    val cosineR = cosineL
      .map{case(itemA, itemB, weight) => (itemB, itemA, weight)}

    cosineL union cosineR
  }
}
