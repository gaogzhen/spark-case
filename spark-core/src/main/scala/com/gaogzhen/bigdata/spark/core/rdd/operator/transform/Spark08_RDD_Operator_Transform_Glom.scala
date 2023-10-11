package com.gaogzhen.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author gaogzhen
 * @since 2023/10/10 22:23
 */
object Spark08_RDD_Operator_Transform_Glom {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    // 本地文件
    val rdd = sc.makeRDD(List(1,2, 3, 4), 2)

    val glomRdd: RDD[Array[Int]] = rdd.glom()

    val maxRdd: RDD[Int] = glomRdd.map(_.max)
    println(maxRdd.collect().sum)
    // 关闭环境
    sc.stop()
  }
}
