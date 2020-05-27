package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark33_Oper32_aggregate {
  def main(args: Array[String]): Unit = {
    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

    val result: Int = listRDD.aggregate(0)(_ + _, _ + _)
    println(result)

    /**
     * 如果这样写，分析一下
     * 两个分区，每个分区加一下初始值，结果应该是 55+20=75
     * todo 但是下面的结果是 85，这是怎么回事呢？
     *      这是因为 aggregate 算子在分区内计算的时候会加上初始值10，分区间计算的时候也会加上初始值10，这样就加了 3 次，所以是 85
     */
    val result2: Int = listRDD.aggregate(10)(_ + _, _ + _)
    println(result2)


  }
}
