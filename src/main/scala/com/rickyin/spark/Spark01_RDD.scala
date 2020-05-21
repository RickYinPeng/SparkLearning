package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {
  def main(args: Array[String]): Unit = {


    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc : SparkContext = new SparkContext(wordCountConf)

    /**
     * 创建RDD
     */
    // 1.从内存中创建
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // 2.从内存中创建 parallelize
    val arrayRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    // 3.从外部存储中创建
    // 默认情况下，可以读取项目路径，也可以读取其他路径：HDFS
    // 默认从文件中读取的数据都是字符串类型
    // 读取文件时，传递的分区参数为最小分区数，但是不一定是这个分区数，取决与 hadoop 读取文件时分片规则
    val fileRDD: RDD[String] = sc.textFile("in")
    /**
     * val fileRDD: RDD[String] = sc.textFile("in",2)
     * 如上所示，代表最少分区数为2，说的是最少，并不是分区数为2，上面的代码我们调用 saveAsTextFile 后，最终分区为3个文件。
     * todo 为什么是 3 个呢？
     * in文件夹下的文件内容是 12345，因为我们的分区数为2，所以就会两两分区，1和2分一个区，3和4分一个区，5一个分一个区，所以是3个分区。
     * todo 但是，当我们打开这三个分区文件查看里面的内容时发现，12345这些数据全在第一个分区，其他两个分区都没有数据，这是为什么呢？
     * 因为RDD分区的方式和数据存储的方式不一样，那到底数据放在哪个分区取决于Hadoop的分片规则
     *  Hadoop分片规则:
     *    - 首先Hadoop的文件读取是按照行来读取的，不能破环某一行数据(如果破坏了某一行数据分到不同分区，那么WordCount都不能实现)
     */

    //listRDD.collect().foreach(println)

    //将RDD的数据保存到文件中
    //listRDD.saveAsTextFile("output")

    fileRDD.saveAsTextFile("output2")
  }
}
