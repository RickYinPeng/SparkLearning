package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark34_Oper33_fold {
  def main(args: Array[String]): Unit = {
    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

    /**
     * fold算子和前面说的 aggregate 算子一样，只不过是分区内和分区间的计算相同，下面的结果也是85
     */
    val result: Int = listRDD.fold(10)(_ + _)
    println(result)
  }
}
