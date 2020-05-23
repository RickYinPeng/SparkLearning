package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_Oper19_reduceByKey {
  def main(args: Array[String]): Unit = {
    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val words = Array("one", "two", "two", "three", "three", "three")

    val wordPairsRDD: RDD[(String, Int)] = sc.parallelize(words).map(word => (word, 1))

    //相同的key才能放在一块
    val reduceRDD: RDD[(String, Int)] = wordPairsRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 5, 2)
    //listRDD.saveAsTextFile("output6")
    val i: Int = listRDD.reduce(_ + _)
    println(i)
  }
}
