package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark25_Oper24_mapValues {
  def main(args: Array[String]): Unit = {
    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val listRDD: RDD[(Int, String)] = sc.makeRDD(Array((3, "dd"), (6, "cc"), (2, "bb"), (1, "dd")))

    val mapValueRDD: RDD[(Int, String)] = listRDD.mapValues(_ + "|||")

    mapValueRDD.collect().foreach(println)
  }
}
