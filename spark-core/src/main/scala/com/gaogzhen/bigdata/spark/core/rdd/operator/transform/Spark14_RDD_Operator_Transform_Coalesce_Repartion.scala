package com.gaogzhen.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author gaogzhen
 * @since 2023/10/10 22:23
 */
object Spark14_RDD_Operator_Transform_Coalesce_Repartion {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    // coalesce方法默认不会将分区数据打乱重新组合
    // 上述情况缩减分区导致数据不均衡
    // shuffle可以让数据均衡
    // 扩大分区，需要shuffle设置为true

    // val rdd1: RDD[Int] = rdd.coalesce(2)
    // val rdd1: RDD[Int] = rdd.coalesce(2, true)
    val rdd1: RDD[Int] = rdd.repartition(3)
    rdd1.saveAsTextFile("output")
    // 关闭环境
    sc.stop()
  }
}
