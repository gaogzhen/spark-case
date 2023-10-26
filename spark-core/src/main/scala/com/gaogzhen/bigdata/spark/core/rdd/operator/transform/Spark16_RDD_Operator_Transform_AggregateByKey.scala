package com.gaogzhen.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author gaogzhen
 * @since 2023/10/10 22:23
 */
object Spark16_RDD_Operator_Transform_AggregateByKey {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)
    val rdd1: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    val rdd2: RDD[(String, Int)] = rdd1.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    rdd2.foreach(println)
    // 关闭环境
    sc.stop()
  }
}
