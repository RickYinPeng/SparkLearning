package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_Oper23_sortByKey {
  def main(args: Array[String]): Unit = {
    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val listRDD: RDD[(Int, String)] = sc.makeRDD(Array((3, "dd"), (6, "cc"), (2, "bb"), (1, "dd")))

    /**
     * true: 升序
     * false: 降序
     */
    val sortByKeyRDD: RDD[(Int, String)] = listRDD.sortByKey(true)

    val sortByKeyRDD2: RDD[(Int, String)] = listRDD.sortByKey(false)

    sortByKeyRDD.collect().foreach(println)
    sortByKeyRDD2.collect().foreach(println)

  }
}
