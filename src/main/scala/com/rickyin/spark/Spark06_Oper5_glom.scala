package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Oper5_glom {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    //map算子
    /**
     * 创建一个4个分区的RDD
     * numSlices:分区数
     */
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)

    //将一个分区的数据放到一个数组中
    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    glomRDD.collect().foreach(array => {
      array.foreach(x => print(x+" "))
      println()
    })
  }
}
