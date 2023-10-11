package com.gaogzhen.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author gaogzhen
 * @since 2023/10/10 22:23
 */
object Spark09_RDD_Operator_Transform_Groupby {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    // 本地文件
    val rdd = sc.makeRDD(List("Hello", "Java", "Python", "Hadoop"), 2)
    val gbRdd: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

    gbRdd.collect().foreach(println)
    // 分区与分组没有必然的联系
    // 关闭环境
    sc.stop()
  }
}
