package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Oper3_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc : SparkContext = new SparkContext(wordCountConf)

    //map算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    /**
     * 我们一般在参数比较多的情况下会使用模式匹配，模式匹配一般的写法就是将函数的 "()" 变为 "{}" 因为可能有多行
     */
    val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_, "分区号: " + num))
      }
    }
    /**
     * 不使用模式匹配
     */
    val value: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex((num, datas) => {
      datas.map((_, "分区号: " + num))
    })
    value.collect().foreach(println)
  }
}
