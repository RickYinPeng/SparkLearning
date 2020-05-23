package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark18_Oper17_partitionBy {
  def main(args: Array[String]): Unit = {

    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    val rdd: RDD[(Int, String)] = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)

    println("分区前的分区数: " + rdd.partitions.size)

    //对RDD进行重新分区，我们需要传入一个分区器，并传入对应新的分区数
    //todo 注意：如果想要使用 partitionBy 算子，rdd的数据类型必须是 K-V 格式的，如果你的RDD类型只是一个 List(1,2,3,4,5),那么就不能使用 partitionBy 算子
    //todo 为什么我们的RDD数据类型是 List(1,2,3) 我们不能调用 partitionBy 算子，因为隐式转换
    val rdd2: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))

    println("分区后的分区数: " + rdd2.partitions.size)

  }
}

//自定义分区器
//继承 Partitioner 类
class MyPartitioner(partitions: Int) extends Partitioner {

  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = {
    1
  }
}