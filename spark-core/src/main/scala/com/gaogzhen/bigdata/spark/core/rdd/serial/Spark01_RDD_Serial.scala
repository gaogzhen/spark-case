package com.gaogzhen.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 取每个省份每个广告点击量top3
 * @author gaogzhen
 * @since 2023/10/24
 */
object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "java"))

    val search = new Search("h")

    search.getMatch1(rdd).collect().foreach(println)
    // 关闭环境
    sc.stop()
  }

  class Search(query: String) extends Serializable {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    /**
     * 序列化测试1
     *  类的构造参数是类的属性，构造参数需要进行闭包检测
     * @param rdd
     * @return
     */
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    /**
     * 序列化测试2
     * @param rdd
     * @return
     */
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter(_.contains(query))
    }
  }
}
