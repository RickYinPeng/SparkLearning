package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_Oper20_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val wordCountConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(wordCountConf)

    /**
     * 分区内：同一个分区内的操作，称之为分区内操作
     * 分区间：不同分区之间的操作称之为分区键操作
     * 如下listRDD，共有两个分区，第一个分区：1，2 第二个分区：3，4，5
     * 我们可以使用reduce操作来进行求和的运算，因为数据分布在不同分区，我们需要先在分区内进行相加操作，然后分区间进行相加操作
     * reduce算子就是这样
     * todo 那如果我们的业务是分区内相加，分区间相减的操作，那我们该如何做呢？
     */
    val listRDD: RDD[Int] = sc.makeRDD(1 to 5, 2)
    //listRDD.saveAsTextFile("output6")
    val i: Int = listRDD.reduce(_ + _)
    println(i)

    /**
     * aggregateByKey
     */
    val listRDD2: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    val resultRDD: Array[Array[(String, Int)]] = listRDD2.glom().collect()

    /**
     * 如下代码打印结果如下，分别是两个分区内的内容
     * 分区一：(a,3)(a,2)(c,4)
     * 分区二：(b,3)(c,6)(c,8)
     */
    resultRDD.foreach(x => {
      x.foreach(print(_))
      println()
    })

    val aggRDD: RDD[(String, Int)] = listRDD2.aggregateByKey(0)(math.max(_, _), _ + _)

    aggRDD.collect().foreach(println)

    /**
     * 使用 aggregateByKey 实现 WordCount
     * 分区内相加，然后分区间也相加
     */
    val aggRDD2: RDD[(String, Int)] = listRDD2.aggregateByKey(0)(_ + _, _ + _)
  }
}
