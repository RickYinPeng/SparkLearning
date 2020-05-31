package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark26_Oper25_join {
  def main(args: Array[String]): Unit = {
    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val listRDD: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))

    val listRDD2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (3, 6)))

    val joinRDD: RDD[(Int, (String, Int))] = listRDD.join(listRDD2)

    joinRDD.collect().foreach(println)

    val listRDD3: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaaa"), (2, "bbbb"), (3, "cccc")))
  }
}
