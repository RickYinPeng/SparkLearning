package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_Oper15_cartesian {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    // 创建第一个RDD
    val rdd1: RDD[Int] = sc.makeRDD(1 to 3)

    //创建第二个RDD
    val rdd2: RDD[Int] = sc.makeRDD(2 to 5)

    //计算第一个RDD和第二个RDD的笛卡尔积
    val rdd3: RDD[(Int, Int)] = rdd1.cartesian(rdd2)

    rdd3.collect().foreach(println)
  }
}
