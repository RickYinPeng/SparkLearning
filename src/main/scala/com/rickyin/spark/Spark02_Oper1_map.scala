package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper1_map {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc : SparkContext = new SparkContext(wordCountConf)

    //map算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    //创建Spark上下文对象的应用程序称为 Driver
    //todo 所有RDD算子的计算功能全部由 Executor 执行（这里面的 x * 2 会由 Driver 发往相应的 Executor）
    //todo 那如果 map 函数中使用了外部的变量会怎么样呢？如下
    //     可以正常打印结果，证明可以这样写，但是我们得思考，如果使用了外部变量，我们的外部变量是在Driver中声明的，如果Exectuor中使用
    //     了外部的变量，那么就需要 Driver 将该变量传送给相应的 Executor ，那么这个时候就涉及了网络IO，也就是说，你的这个外部变量必须
    //     可以序列化才能在网络上传输，否则就会出错
    val i = 10
    val value: RDD[Int] = listRDD.map(x => x * i)
    val mapRDD: RDD[Int] = listRDD.map(x => x * 2) // (_*2)

    value.collect().foreach(println)
    mapRDD.collect().foreach(println)
  }
}
