package com.gaogzhen.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author gaogzhen
 * @since 2023/10/10 22:23
 */
object Spark07_RDD_Operator_Transform_FlatMap {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    // 本地文件
    val rdd = sc.makeRDD(List(List(1,2), 3, List(4, 5)))
    val flatRdd = rdd.flatMap(data => {
      data match {
        case list: List[_] => list
        case data => List(data)
      }
    })

    flatRdd.collect().foreach(println)
    // 关闭环境
    sc.stop()
  }
}
