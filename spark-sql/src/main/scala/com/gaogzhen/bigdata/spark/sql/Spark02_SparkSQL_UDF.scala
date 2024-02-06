package com.gaogzhen.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 *
 * @author gaogzhen
 * @since 2023/11/8 07:38
 */
object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {

    // 创建SparkSQL的运行环境
    val sc: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()
    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    import spark.implicits._

    val df: DataFrame = spark.read.json("data/sql/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("prefixName", "Name: " + _)
    spark.sql("select age, prefixName(username) from user").show


    // 关闭环境
    spark.close()
  }


}
