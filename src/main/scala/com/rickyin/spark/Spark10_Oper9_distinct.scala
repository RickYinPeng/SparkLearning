package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_Oper9_distinct {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))

    /**
     * 使用distinct算子对数据去重，但是因为去重后会导致数据减少，所以可以改变默认的分区数量
     */
    //val distinctRDD: RDD[Int] = listRDD.distinct()
    val distinctRDD: RDD[Int] = listRDD.distinct(2)

    //distinctRDD.collect().foreach(println)

    //distinctRDD.saveAsTextFile("output3")
    distinctRDD.saveAsTextFile("output4")
  }
}
