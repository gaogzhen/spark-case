package com.gaogzhen.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author gaogzhen
 * @since 2023/10/9 07:47
 */
object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    // 本地文件
    // val rdd = sc.textFile("data/1*.txt")
    val rdd = sc.wholeTextFiles("data")
    // 分布式存储系统路径：HDFS
    // val rdd = sc.textFile("hdfs://node1:8020/words.txt")
    rdd.collect().foreach(println)
    // 关闭环境
    sc.stop()
  }
}
