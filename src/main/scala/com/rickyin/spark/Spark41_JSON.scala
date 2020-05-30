package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json.JSON

object Spark41_JSON {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    //读取文件
    val json: RDD[String] = sc.textFile("in/user.json")

    //解析json数据
    val result: RDD[Option[Any]] = json.map(JSON.parseFull)

    result.foreach(println)

    //释放资源
    sc.stop()
  }
}

