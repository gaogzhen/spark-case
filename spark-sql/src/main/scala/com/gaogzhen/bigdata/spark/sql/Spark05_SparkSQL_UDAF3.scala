package com.gaogzhen.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn, functions}

/**
 *
 * @author gaogzhen
 * @since 2023/11/8 07:38
 */
object Spark05_SparkSQL_UDAF3 {
  def main(args: Array[String]): Unit = {

    // 创建SparkSQL的运行环境
    val sc: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()
    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    import spark.implicits._
    val df: DataFrame = spark.read.json("data/sql/user.json")

    // 早期版本，spark不能再sql中使用强类型的UDAF操作
    // SQL&DSL
    // 早期UDAF强类型聚合函数使用DSL语法操作

    val ds: Dataset[User] = df.as[User]

    // 将UDAF函数转换为查询的列对象
    val udafCol: TypedColumn[User, Long] = new MyAvgUDAF().toColumn

    ds.select(udafCol).show


    // 关闭环境
    spark.close()
  }

  /**
   * 自定义聚合函数
   * 1. 继承org.apache.spark.sql.expressions.Aggregator
   *  IN：输入类型Long
   *  BUF：缓冲区类型Buff
   *  OUT：输出数据类型Long
   * 2. 重写方法(6)
   */
  case class User( username: String, age: Long)
  case class Buff(var total: Long, var count: Long)
  class MyAvgUDAF extends Aggregator[User, Buff, Long] {
    // 初始值或者零值
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 根据输入更新缓冲区
    override def reduce(b: Buff, u: User): Buff = {
      b.total = b.total + u.age
      b.count = b.count + 1
      b
    }

    // 缓冲区合并
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      b1.count = b1.count + b2.count
      b1
    }

    // 计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    // 缓冲区编码
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
