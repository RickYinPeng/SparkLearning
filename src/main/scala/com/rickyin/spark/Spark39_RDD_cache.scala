package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkConf, SparkContext}

object Spark39_RDD_cache {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    //创建一个RDD
    val arrRDD: RDD[String] = sc.makeRDD(Array("rickyin"))

    //将RDD转换为携带当前时间戳不做缓存
    val noCacheRDD: RDD[String] = arrRDD.map(_.toString + System.currentTimeMillis())
    noCacheRDD.collect.foreach(println) // rickyin1590809164660
    noCacheRDD.collect.foreach(println) // rickyin1590809164715
    noCacheRDD.collect.foreach(println) // rickyin1590809164740

    //将RDD转换为携带当前时间戳并做缓存(下面的结果是从之前的缓存中拿取的)
    val cacheRDD: RDD[String] = arrRDD.map(_.toString + System.currentTimeMillis()).cache
    cacheRDD.collect.foreach(println) // rickyin1590809287209
    cacheRDD.collect.foreach(println) // rickyin1590809287209
    cacheRDD.collect.foreach(println) // rickyin1590809287209

    val debugString: String = cacheRDD.toDebugString
    println(debugString)
    //释放资源
    sc.stop()
  }
}

