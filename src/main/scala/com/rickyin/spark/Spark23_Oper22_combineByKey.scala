package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_Oper22_combineByKey {
  def main(args: Array[String]): Unit = {
    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    val combineRDD: RDD[(String, (Int, Int))] = listRDD.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    combineRDD.collect().foreach(println)

    /**
     * 使用模式匹配的写法
     */
    val value1: RDD[(String, Int)] = combineRDD.map { case (key, value) => (key, value._1 / value._2) }

    value1.collect().foreach(println)

    println("--------------------------")
    /**
     * 原始写法
     */
    val resultRDD: RDD[(String, Int)] = combineRDD.map((x: (String, (Int, Int))) => {
      (x._1, x._2._1 / x._2._2)
    })
    resultRDD.collect().foreach(println)

    /**
     * 使用 combineByKey 实现 WordCount
     */
    val wordCountRDD: RDD[(String, Int)] = listRDD.combineByKey(x => x, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y)
    wordCountRDD.collect().foreach(println)
  }
}
