package com.gaogzhen.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author gaogzhen
 * @since 2023/10/9 07:47
 */
object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)
    // val rdd: RDD[Int] = sc.parallelize(seq)
    val rdd: RDD[Int] = sc.makeRDD(seq)
    rdd.collect().foreach(println)
    // 关闭环境
    sc.stop()
  }
}
