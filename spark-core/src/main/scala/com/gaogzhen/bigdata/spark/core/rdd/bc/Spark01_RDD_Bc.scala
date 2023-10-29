package com.gaogzhen.bigdata.spark.core.rdd.bc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 取每个省份每个广告点击量top3
 *
 * @author gaogzhen
 * @since 2023/10/24
 */
object Spark01_RDD_Bc {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val map: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    // 封装广播变量：executor内共享只读
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd.map{
      case (w, c) => {
        val n: Int = bc.value.getOrElse(w, 0)
        (w, (c, n))
      }
    }.collect().foreach(println)
  }
}
