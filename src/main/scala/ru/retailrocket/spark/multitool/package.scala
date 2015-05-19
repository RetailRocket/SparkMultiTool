package ru.retailrocket.spark

import scala.reflect.ClassTag
import scala.util._

import org.apache.spark.rdd._


package object multitool {
  object Functions {
    def tap[T:ClassTag](f: T => Unit)(o: T) = {f(o); o}
    def applyIf[T:ClassTag](p: Boolean)(f: T => T)(o: T): T = {if(p) f(o) else o}

    def maxBy[T,O<%Ordered[O]](f:T=>O)(a:T, b:T) = if(f(a)>f(b)) a else b
    def minBy[T,O<%Ordered[O]](f:T=>O)(a:T, b:T) = if(f(a)<f(b)) a else b

    def sumTuple2[T](a: (T,T), b: (T,T))(implicit num: Numeric[T]): (T,T) = {
      import num._
      (a._1+b._1, a._2+b._2)
    }

    def sumTuple3[T](a: (T,T,T), b: (T,T,T))(implicit num: Numeric[T]): (T,T,T) = {
      import num._
      (a._1+b._1, a._2+b._2, a._3+b._3)
    }
  }

  object Implicits {
    implicit class MultitoolFunctionsImplicits[T:ClassTag](val self: T) {
      def tap(f: T => Unit) = Functions.tap(f)(self)
      def applyIf(p: Boolean)(f: T => T): T = Functions.applyIf(p)(f)(self)
    }

    implicit class MultitoolRDDFunctionsImplicits[T:ClassTag](val self: RDD[T]) {
      def transform[R:ClassTag](f: T=>Option[R]): RDDFunctions.TransformResult[T,R] = {
        RDDFunctions.transform(f)(self)
      }
      def transform[R:ClassTag](f: T=>R)(implicit d: DummyImplicit): RDDFunctions.TransformResult[T,R] = {
        RDDFunctions.transform(f)(self)
      }
    }
  }
}
