package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Oper8_sample {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    // 从指定的数据集合中进行抽样处理，根据不同的算法进行抽样
    /**
     * false:不放回
     * true: 放回
     */
    val sampleRDD: RDD[Int] = listRDD.sample(false, 0.6, 1)

    sampleRDD.collect().foreach(println)

  }
}
