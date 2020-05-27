package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark28_Oper27_reduce {
  def main(args: Array[String]): Unit = {
    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

    val i: Int = listRDD.reduce(_ + _)
    println(i)

    val mapRDD: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 3), ("c", 5), ("d", 5)))

    val tuple: (String, Int) = mapRDD.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    println(tuple)
  }
}
