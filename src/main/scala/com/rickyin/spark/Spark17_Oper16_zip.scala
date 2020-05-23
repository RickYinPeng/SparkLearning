package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_Oper16_zip {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    // 创建第一个RDD
    val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,3),3)

    //创建第二个RDD
    val rdd2: RDD[String] = sc.makeRDD(Array("a", "b", "c"), 3)

    //计算第一个RDD和第二个RDD的拉链zip
    /**
     * Spark中的拉链：
     *      1.必须保证每个RDD中的分区数相等
     *      2.必须保证每个分区中的数据相同
     */
    val rdd3: RDD[(Int, String)] = rdd1.zip(rdd2)

    rdd3.collect().foreach(println)
  }
}
