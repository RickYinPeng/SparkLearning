package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark27_Oper26_cogroup {
  def main(args: Array[String]): Unit = {
    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val listRDD: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))

    val listRDD2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (3, 6)))

    val cogroupRDD: RDD[(Int, (Iterable[String], Iterable[Int]))] = listRDD.cogroup(listRDD2)

    cogroupRDD.collect().foreach(println)
  }
}
