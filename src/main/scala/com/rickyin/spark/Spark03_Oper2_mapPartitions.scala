package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Oper2_mapPartitions {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc : SparkContext = new SparkContext(wordCountConf)

    //map算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    // mapPartitions 可以对一个 RDD 中所有的分区进行遍历
    // mapPartitions 的效率优于 map 算子，减少了发送到执行器执行交互的次数
    // mapPartitions 可能会出现内存溢出(OOM)
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {
      datas.map(data => data * 2)
    })

    mapPartitionsRDD.collect().foreach(println)
  }
}
