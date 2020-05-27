package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark29_Oper28_collect {
  def main(args: Array[String]): Unit = {
    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

    /**
     * 收集操作
     */
    val array: Array[Int] = listRDD.collect()

    println(array)
  }
}
