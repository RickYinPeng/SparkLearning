package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_Oper18_groupByKey {
  def main(args: Array[String]): Unit = {
    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val words = Array("one", "two", "two", "three", "three", "three")

    val wordPairsRDD: RDD[(String, Int)] = sc.parallelize(words).map(word => (word, 1))

    val groupRDD: RDD[(String, Iterable[Int])] = wordPairsRDD.groupByKey()

    groupRDD.collect().foreach(println)

    groupRDD.map(t => (t._1,t._2.sum)).collect().foreach(println)

  }
}
