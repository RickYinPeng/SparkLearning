package com.rickyin.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkConf, SparkContext}

object Spark38_Lineage {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val mapRDD: RDD[(Int, Int)] = listRDD.map((_, 1))

    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)

    //查看当前 reduceRDD 的依赖关系
    val string: String = reduceRDD.toDebugString

    //查看 reduceRDD 的依赖类型
    val dependencies: Seq[Dependency[_]] = reduceRDD.dependencies
    println(string)
    println(dependencies)

    //释放资源
    sc.stop()
  }
}

