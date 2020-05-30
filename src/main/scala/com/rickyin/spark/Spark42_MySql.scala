package com.rickyin.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object Spark42_MySql {
  def main(args: Array[String]): Unit = {

    //1.创建Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://47.93.62.45:3306/rdd"
    val userName = "root"
    val passWd = "root"

    /**
     * todo 查询数据
     */
    val sql = "select name,age from user where id >= ? and id <= ?"

    //4.创建jdbcRDD,访问数据库
    val jdbcRDD = new JdbcRDD(
      sc,
      () => {
        //获取数据库的连接对象
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      sql,
      1,
      3,
      2,
      (rs) => {
        println(rs.getString(1) + "," + rs.getInt(2))
      }
    )
    jdbcRDD.collect()

    /**
     * todo 保存数据
     */
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zhangsan", 20), ("wangwu", 38), ("lisi", 22)))

    /**
     * 以下代码为向MySQL插入数据的代码，有没有发现什么问题？
     * 我们在foreach循环中创建了连接对象 Connection ，那么是不是我们每次循环的时候都会创建连接对象，有100个数据就会创建100个连接对象
     * 那这样是不是会很浪费资源，那我们怎么解决呢？ 我们将创建Connection对象的代码拿出来不就行了
     */
    /*
    dataRDD.foreach {
      case (username, age) => {
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)
        val sql = "insert into user (name,age) values(?,?)"
        val statement: PreparedStatement = connection.prepareStatement(sql)
        statement.setString(1, username)
        statement.setInt(2, age)
        statement.executeUpdate()
        statement.close()
        connection.close()
      }
    }
    */

    /**
     * 如下所示，为我们将创建 Connection 连接的代码放到外面，我们执行下面代码，会报错，说 Connection对象无法序列化？
     * 因为发往 Executor 端的代码使用了 Connection 的方法，所以需要 Connection 的序列化，但是 Connection 是人家写的啊，我们怎么改呢？
     */
    /*
    Class.forName(driver)
    val connection: Connection = DriverManager.getConnection(url, userName, passWd)
    dataRDD.foreach {
      case (username, age) => {

        val sql = "insert into user (name,age) values(?,?)"
        val statement: PreparedStatement = connection.prepareStatement(sql)
        statement.setString(1, username)
        statement.setInt(2, age)
        statement.executeUpdate()
        statement.close()

      }
    }
    connection.close()
     */


    /**
     * 另外一种方式解决上面的问题
     */
    dataRDD.foreachPartition(datas => {
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)
      datas.foreach {
        case (username, age) => {
          val sql = "insert into user (name,age) values(?,?)"
          val statement: PreparedStatement = connection.prepareStatement(sql)
          statement.setString(1, username)
          statement.setInt(2, age)
          statement.executeUpdate()
          statement.close()
        }
      }
      connection.close()
    })

    //释放资源
    sc.stop()
  }
}

