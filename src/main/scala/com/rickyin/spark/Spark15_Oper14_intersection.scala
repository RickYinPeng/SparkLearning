package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_Oper14_intersection {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    // 创建第一个RDD
    val rdd1: RDD[Int] = sc.makeRDD(1 to 7)

    //创建第二个RDD
    val rdd2: RDD[Int] = sc.makeRDD(5 to 10)

    //计算第一个RDD和第二个RDD的交集
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
    rdd3.collect().foreach(println)

  }
}
