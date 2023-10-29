package com.gaogzhen.bigdata.spark.core.rdd.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 取每个省份每个广告点击量top3
 * @author gaogzhen
 * @since 2023/10/24
 */
object Spark01_RDD_Acc {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    var sum: LongAccumulator = sc.longAccumulator("sum")

    val list = List(1, 2, 3, 4)
    val rdd: RDD[Int] = sc.makeRDD(list)

    val mapRdd: RDD[Int] = rdd.map(n => {
      sum.add(n)
      n
    })

    // 少加：如果没有行动算子，不会执行
    // 多加：行动算子被执行多次，会多次执行计算
    mapRdd.collect()
    mapRdd.collect()

    println(sum.value)
  }
}
