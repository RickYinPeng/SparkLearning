package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_Oper10_coalesce {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    // 缩减分区：可以简单的理解为合并分区
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16,4)

    println("缩减分区前的分区数="+listRDD.partitions.size)

    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)
    println("缩减分区后的分区数="+coalesceRDD.partitions.size)
    coalesceRDD.saveAsTextFile("output5")
  }
}
