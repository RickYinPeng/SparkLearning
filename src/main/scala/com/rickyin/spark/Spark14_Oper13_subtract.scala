package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_Oper13_subtract {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    // 创建第一个RDD
    val rdd1: RDD[Int] = sc.makeRDD(3 to 8)

    //创建第二个RDD
    val rdd2: RDD[Int] = sc.makeRDD(1 to 5)

    //计算第一个RDD和第二个RDD的差集并打印
    val rdd3: RDD[Int] = rdd1.subtract(rdd2)

    val rdd4: RDD[Int] = rdd2.subtract(rdd1)

    rdd3.collect().foreach(println)
    println("--------------------")
    rdd4.collect().foreach(println)
  }
}
