package com.gaogzhen.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author gaogzhen
 * @since 2023/10/10 22:23
 */
object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    // 本地文件
    val rdd = sc.makeRDD(List(1,2,3,4), 2)
    // mapPartitions：以分区为单位进行数据转换操作
    // 但是会整个分区数据加载到内存，处理完数据不会被释放，存在内存引用
    // 在内存小，数据量大的情况下可能造成内存溢出oom
    val mapRdd1: RDD[Int] = rdd.mapPartitions(iter => {
      println(">>>>")
      iter.map(_*2)
    })

    mapRdd1.collect().foreach(println)
    // 关闭环境
    sc.stop()
  }
}
