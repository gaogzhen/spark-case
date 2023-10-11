package com.gaogzhen.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author gaogzhen
 * @since 2023/10/10 22:23
 */
object Spark11_RDD_Operator_Transform_Filter {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    // 本地文件
    val fileRdd: RDD[String] = sc.textFile("data/apache.log")
    val ret = fileRdd.filter(_.split(" ")(3).startsWith("17/05/2015"))


    ret.collect().foreach(println)
    // 关闭环境
    sc.stop()
  }
}
