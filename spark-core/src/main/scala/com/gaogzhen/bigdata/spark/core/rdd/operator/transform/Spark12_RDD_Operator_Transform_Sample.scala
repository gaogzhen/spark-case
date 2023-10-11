package com.gaogzhen.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author gaogzhen
 * @since 2023/10/10 22:23
 */
object Spark12_RDD_Operator_Transform_Sample {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // sample 算子传递3个参数
    // 1. 抽取数据后是否将数据返回 true（返回），false（不返回）
    // 2. 数据源中每条数据抽取的的概率
    // 3. 抽取数据时随机算法的种子，如果不传递使用当前系统时间
    val sampleRdd: RDD[Int] = rdd.sample(false, 0.4, 1)
    // val sampleRdd: RDD[Int] = rdd.sample(true, 2)

    println(sampleRdd.collect().mkString(","))
    // 关闭环境
    sc.stop()
  }
}
