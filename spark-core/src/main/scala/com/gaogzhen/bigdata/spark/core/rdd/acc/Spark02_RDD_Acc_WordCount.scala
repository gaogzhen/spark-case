package com.gaogzhen.bigdata.spark.core.rdd.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 取每个省份每个广告点击量top3
 *
 * @author gaogzhen
 * @since 2023/10/24
 */
object Spark02_RDD_Acc_WordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    // 创建自定义累加器
    val wcAcc = new MyAccumulator()
    // 注册累加器
    sc.register(wcAcc, "wordAcc")

    val list = List("hello", "java", "spark", "spark")
    val rdd: RDD[String] = sc.makeRDD(list)

    rdd.foreach( wcAcc.add(_))



    println(wcAcc.value)
  }

  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

   private var wcMap = mutable.Map[String, Long]()

    // 判断是否初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    // 重置累加器
    override def reset(): Unit = {
      wcMap.clear()
    }

    // 累加
    override def add(v: String): Unit = {
      val newVal: Long = wcMap.getOrElse(v, 0L) + 1
      wcMap.update(v, newVal)
    }

    // 合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach {
        case (word, count) => {
          val newVal = map1.getOrElse(word, 0L) + count
          map1.update(word, newVal)
        }
      }
    }

    // 累加结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}
