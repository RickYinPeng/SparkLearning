package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 检查点
 */
object Spark40_RDD_checkPoint {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    // 1.创建rdd
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 2.对rdd进行一次转换
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    // 6.使用检查点，然后再看血缘变化。(注意：如果我们要使用检查点机制,需要先设置检查点的保存目录,不然会报错)
    sc.setCheckpointDir("cp")
    mapRDD.checkpoint()

    // 3.对rdd进行第二次转换
    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)

    // 4.打印结果
    reduceRDD.foreach(println)

    // 5.查看其血缘
    println(reduceRDD.toDebugString)

    //释放资源
    sc.stop()
  }
}

