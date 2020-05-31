package com.rickyin.spark

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

//自定义累加器
object Spark44_Accumulator {
  def main(args: Array[String]): Unit = {

    //1.创建Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ShareData")

    //2.创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[String] = sc.makeRDD(List("Hadoop", "Hive", "Hbase", "Scala", "Spark"), 2)

    //TODO 1.创建累加器
    val wordAccumulator = new WordAccumulator
    //todo 2.注册累加器
    sc.register(wordAccumulator)

    dataRDD.foreach{
      case word => {
        wordAccumulator.add(word)
      }
    }

    println("累加器结果："+wordAccumulator.value)

    //释放资源
    sc.stop()
  }
}

//声明累加器
// 1. 继承 AccumulatorV2
// 2. 实现抽象方法
// 3. 创建累加器
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  val list = new util.ArrayList[String]()

  // 当前的累加器是否为初始化状态
  override def isZero: Boolean = list.isEmpty

  // 复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator
  }

  // 重置累加器对象
  override def reset(): Unit = list.clear()

  // 向累加器中增加数据
  override def add(v: String): Unit = {
    if (v.contains("H")) {
      list.add(v)
    }
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  // 获取累加器的结果
  override def value: util.ArrayList[String] = list
}
