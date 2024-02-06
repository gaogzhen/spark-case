package com.gaogzhen.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 *
 * @author gaogzhen
 * @since 2023/11/8 07:38
 */
object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {

    // 创建SparkSQL的运行环境
    val sc: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()
    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    import spark.implicits._
    // 执行逻辑操作
    // ============= DataFrame =========
    // val df: DataFrame = spark.read.json("data/sql/user.json")
    // df.show()
    // DataFrame => SQL
    // df.createOrReplaceTempView("user")
    //
    // spark.sql("select * from user").show
    // spark.sql("select age from user").show
    // spark.sql("select avg(age) from user").show
    // DataFrame => DSL
    // df.select("age", "username").show
    // df.select('age + 1).show
    // ========= DataSet ===========
    // val seq: Seq[Int] = Seq(1, 2, 3, 4)
    // val ds: Dataset[Int] = seq.toDS()
    // ds.show()
    // ========= RDD <=> DataFrame ========
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df: DataFrame = rdd.toDF("id", "username", "age")
    val rowRdd: RDD[Row] = df.rdd
    // =======  DataFrame <=> DataSet ========
    val ds: Dataset[User] = df.as[User]
    val df1: DataFrame = ds.toDF()
    // ========== Rdd <=> DataSet ======
    val ds1: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()

    val userRdd: RDD[User] = ds1.rdd

    // 关闭环境
    spark.close()
  }

  case class User(id:Int, username: String, age: Int)

}
