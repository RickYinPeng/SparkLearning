package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_Oper12_union {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    // 创建第一个RDD
    val rdd1: RDD[Int] = sc.makeRDD(1 to 5)

    //创建第二个RDD
    val rdd2: RDD[Int] = sc.makeRDD(5 to 10)

    //计算两个RDD的并集
    val rdd3: RDD[Int] = rdd1.union(rdd2)

    rdd3.collect().foreach(println)
  }
}
