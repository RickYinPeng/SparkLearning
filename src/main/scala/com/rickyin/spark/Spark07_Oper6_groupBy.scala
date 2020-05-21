package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Oper6_groupBy {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    //生成数据，按照指定的规则分组
    //分组后的数据形成了对偶元组(K-V),K表示分组的key,v表示分组的数据集合
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val groupRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i => i % 2)

    groupRDD.collect().foreach(println)

  }
}
