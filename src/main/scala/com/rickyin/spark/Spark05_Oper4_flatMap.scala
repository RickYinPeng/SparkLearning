package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Oper4_flatMap {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc : SparkContext = new SparkContext(wordCountConf)

    //map算子
    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))

    // flatMap
    val flatMapRDD: RDD[Int] = listRDD.flatMap(datas => datas)

    flatMapRDD.collect().foreach(println)
  }
}
