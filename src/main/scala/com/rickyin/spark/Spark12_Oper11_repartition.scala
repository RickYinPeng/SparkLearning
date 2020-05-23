package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Oper11_repartition {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    // 缩减分区：可以简单的理解为合并分区
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16,4)

    /**
     * 可以选择是否shuffle
     */
    //listRDD.coalesce()

    //listRDD.repartition()

    //listRDD.sortBy()
  }
}
