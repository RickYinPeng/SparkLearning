package com.rickyin.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark43_ShareData {
  def main(args: Array[String]): Unit = {

    //1.创建Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ShareData")

    //2.创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //将dataRDD中的数据进行累加，我们可以使用 reduce算子，轻松做到
    //todo 但是，这样做虽然看起来很简单，但是reduce函数底层涉及的操作却是很复杂的，我们现在有两个分区，将数据发往不同的分区，首先分区内
    //     的数据相加，然后分区间的数据相加，感觉很复杂，我们能不能简化一下，别整那么复杂
    val i: Int = dataRDD.reduce(_ + _)
    println(i)

    //todo 我们可以这样写
    var sum = 0
    dataRDD.foreach(i => sum = sum + i)
    println("普通变量sum的结果："+sum) //todo sum=0，并没有得出我们预想的结果 10，那这是为什么呢？我们来分析一下

    /**
     * todo
     *  1.首先 sum 这个变量是在 Driver 中的
     *  2.现在我们的 RDD 有两个分区，假设分区一中的数据是 1，2；分区二中的数据是 3，4
     *  3.我们的 foreach 中的操作是要在 Executor 端执行的( i => sum = sum + i )
     *  4.所以我们需要将 sum 变量发往两个 Executor 的，现在 sum=0，所以发往两个 Executor 端的 sum 的初始值都是 0
     *  5.Executor1里面进行的操作是: sum=sum+1+2=3
     *  6.Executor2里面进行的操作是: sum=sum+3+4=7
     *  7.第一个问题，我们如何将两个 Executor 中的 sum 的值相加呢？
     *  8.第二个问题，我们现在是在 Driver 端打印的 sum 变量，那现在我们 Executor 端的 sum 有没有往 Driver 端传呢？没有啊，没有办法
     *    聚合回Driver
     *  9.接下来我们使用累加器来共享变量,因为我们的 Driver 和 Executor 端都要使用这个变量。(只写变量)
     */

    //todo 使用共享变量来累加数据，它能将共享变量从 Executor 端返回给 Driver
    //创建累加器变量
    val accumulator: LongAccumulator = sc.longAccumulator
    dataRDD.foreach {
      case i => {
        //执行累加器的累加功能
        accumulator.add(i)
      }
    }
    println("累加器结果：" + accumulator.value)

    //释放资源
    sc.stop()
  }
}

