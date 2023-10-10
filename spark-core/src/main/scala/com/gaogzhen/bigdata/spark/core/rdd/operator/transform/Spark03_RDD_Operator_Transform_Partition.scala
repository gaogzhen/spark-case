package com.gaogzhen.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author gaogzhen
 * @since 2023/10/10 22:23
 */
object Spark03_RDD_Operator_Transform_Partition {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    // 本地文件
    val rdd = sc.makeRDD(List(1,2,3,4), 2)
    val mapRdd1: RDD[Int] = rdd.mapPartitions(num => {
      println(">>>>" + num)
      num
    })
    val mapRdd2: RDD[Int] = mapRdd1.map(num => {
      println("####" + num)
      num
    })
    // 1. rdd 的计算一个分区内数据是一个一个执行
    // 2. 分区内执行是有序的;分区间执行是无序的。并行执行
    mapRdd2.collect()
    // 关闭环境
    sc.stop()
  }
}
