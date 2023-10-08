package com.gaogzhen.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author gaogzhen
 * @since 2023/10/8 20:22
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建链接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    // 读取文件
    val fileRDD: RDD[String] = sc.textFile("data")
    // 统计
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    val word2oneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    val word2Count: RDD[(String, Int)] = word2oneRDD.reduceByKey(_ + _)

    // 打印
    val array: Array[(String, Int)] = word2Count.collect()
    array.foreach(println)
    sc.stop()
  }
}
