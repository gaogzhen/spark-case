package com.gaogzhen.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 取每个省份每个广告点击量top3
 * @author gaogzhen
 * @since 2023/10/24
 */
object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("data/words.txt")
    println(lines.dependencies)
    println("******************")

    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("******************")

    val wordTone: RDD[(String, Int)] = words.map(w => (w, 1))
    println(wordTone.dependencies)
    println("******************")

    val wordToSum: RDD[(String, Int)] = wordTone.reduceByKey(_ + _)
    println(wordToSum.dependencies)
    println("******************")

    val ret: Array[(String, Int)] = wordToSum.collect()
    ret.foreach(println)
  }
}
