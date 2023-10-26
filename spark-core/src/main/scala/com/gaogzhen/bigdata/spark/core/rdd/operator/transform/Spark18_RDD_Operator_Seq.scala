package com.gaogzhen.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 取每个省份每个广告点击量top3
 * @author gaogzhen
 * @since 2023/10/24
 */
object Spark18_RDD_Operator_Seq {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 1.获取原始数据：时间戳 省份 城市 用户 广告
    val dataRDD = sc.textFile("data/agent.log")
    // 2.转换数据结构 时间戳 省份 城市 用户 广告 -> （(省份,广告),点击量)
    val mapRdd: RDD[((String, String), Int)] = dataRDD.map(
      line => {
        val data: Array[String] = line.split(" ")
        ((data(1), data(4)), 1)
      }
    )
    // 3.分组聚合
    val reduceRDD: RDD[((String, String), Int)] = mapRdd.reduceByKey(_ + _)
    // 4.分组聚合后的结构转换：((省份,广告),点击量) -> (省份,(广告,点击量))
    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((prov, ad), num) => {
        (prov, (ad, num))
      }
    }
    // 5.分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()
    // 6.组内排序（降序），取前3名
    val retRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    // 7.采集数据，打印控制台
    retRDD.collect().foreach(println)
    // 关闭环境
    sc.stop()
  }
}
