package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark36_Oper35_countByKey {
  def main(args: Array[String]): Unit = {
    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val mapRDD: RDD[(Int, Int)] = sc.makeRDD(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

    val key: collection.Map[Int, Long] = mapRDD.countByKey

    println(key) // Map(3 -> 2, 1 -> 3, 2 -> 1)

  }
}
