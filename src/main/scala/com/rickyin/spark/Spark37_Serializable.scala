package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark37_Serializable {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    val listRDD: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atrickyin"))

    val search = new Search("h")

    // todo 传递方法，向 Executor 端传递的是 Search对象中的成员方法 isMatch
    //val match1RDD: RDD[String] = search.getMatch1(listRDD)

    // todo 传递属性
    val match2RDD: RDD[String] = search.getMatch2(listRDD)

    //match1RDD.collect().foreach(println)
    match2RDD.collect().foreach(println)

    //释放资源
    sc.stop()
  }
}

class Search(query: String) extends Serializable {

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    val q = query
    rdd.filter(x => x.contains(q))
  }
}
