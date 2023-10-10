package com.gaogzhen.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author gaogzhen
 * @since 2023/10/9 07:47
 */
object Spark03_RDD_Memory_Partition {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    // sparkConf.set("spark.default.parallelism", "5")
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    rdd.saveAsTextFile("output")
    // 关闭环境
    sc.stop()
  }
}
