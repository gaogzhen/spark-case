package com.gaogzhen.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    // 执行逻辑操作
    // DataFrame
    val df: DataFrame = spark.read.json("data/sql/user.json")
    // df.show()
    // DataFrame => SQL
    df.createOrReplaceTempView("user")

    spark.sql("select * from user").show
    spark.sql("select age from user").show
    spark.sql("select avg(age) from user").show

    // DataSet
    // RDD <=> DataFrame
    // DataFrame <=> DataSet
    // Rdd <=> DataSet

    // 关闭环境
    spark.close()
  }

}
