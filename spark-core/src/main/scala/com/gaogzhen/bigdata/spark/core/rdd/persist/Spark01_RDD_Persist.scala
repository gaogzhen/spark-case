package com.gaogzhen.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 取每个省份每个广告点击量top3
 * @author gaogzhen
 * @since 2023/10/24
 */
object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val list = List("hello spark", "hello java")
    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRdd: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = flatRdd.map(word => {
      println("@@@@@@")
      (word, 1)
    })

    mapRdd.cache()
    mapRdd.persist()

    val reduceRdd: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)
    reduceRdd.collect().foreach(println)
    println("============")
    val groupRdd: RDD[(String, Iterable[Int])] = mapRdd.groupByKey()
    groupRdd.collect().foreach(println)
  }
}
