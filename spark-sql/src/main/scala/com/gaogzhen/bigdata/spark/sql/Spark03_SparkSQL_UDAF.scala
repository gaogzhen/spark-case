package com.gaogzhen.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 *
 * @author gaogzhen
 * @since 2023/11/8 07:38
 */
object Spark03_SparkSQL_UDAF {
  def main(args: Array[String]): Unit = {

    // 创建SparkSQL的运行环境
    val sc: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()
    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则

    val df: DataFrame = spark.read.json("data/sql/user.json")
    df.createOrReplaceTempView("user")


    spark.udf.register("ageAvg", new MyAvgUDAF())
    spark.sql("select ageAvg(age) from user").show


    // 关闭环境
    spark.close()
  }

  /**
   * 自定义聚合函数
   * 1. 继承
   * 2. 重写方法(8)
   */
  class MyAvgUDAF extends UserDefinedAggregateFunction {

    // In：输入数据结构
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType)
        )
      )
    }

    // Buffer：缓冲数据结构
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total", LongType),
          StructField("count", LongType)
        )
      )
    }

    // Out：结果类型
    override def dataType: DataType = LongType

    // 函数的稳定性
    override def deterministic: Boolean = true

    // 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    // In -> Buffer：根据输入数据更新缓冲区
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(1) + 1)
    }

    // 缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    // 聚合
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0)/buffer.getLong(1)
    }
  }

}
