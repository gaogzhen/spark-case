package com.gaogzhen.bigdata.spark.core.rdd.req

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * top10热门品类
 *
 * @author gaogzhen
 * @since 2023/10/30
 */
object Spark01_RDD_Analysis {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // 1 读取原始日志数据
    val actionRdd: RDD[String] = sc.textFile("data/user_visit_action.txt")
    actionRdd.cache()

    // 2 注册自定义累加器
    val acc = new HotCategoryAccumulator
    sc.register(acc, "hotCategory")

    // 3 数据累加
    actionRdd.foreach(
      action => {
        val data: Array[String] = action.split("_")
        if (data(6) != "-1") {
          // 点击
          acc.add((data(6), "click"))
        } else if (data(8) != "null") {
          // 下单
          val ids: Array[String] = data(8).split(",")
          ids.foreach(
            id => {
              acc.add((id, "order"))
            }
          )
        } else if (data(10) != "null") {
         // 支付
         val ids: Array[String] = data(10).split(",")
          ids.foreach(
            id => {
              acc.add((id, "pay"))
            }
          )
        }
      }
    )

    // 获取结果
    val accVal: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accVal.map(_._2)

    val sortVal: List[HotCategory] = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt < right.clickCnt) {
          false
        } else if (left.orderCnt > right.orderCnt) {
          true
        } else if (left.orderCnt < right.orderCnt) {
          false
        } else {
          left.payCnt > right.payCnt
        }
      }
    )

    // 排序输出
    sortVal.take(10).foreach(println)
  }

  case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

  /**
   * 自定义累加器
   * 1. 继承AccumulatorV2，定义泛型
   *  IN：（品类ID,行为类型)
   *  OUT：mutable.Map[String, HotCategory]
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

    private val hcMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid: String = v._1
      val actionType: String = v._2
      val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click") {
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      }
      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = hcMap
      val map2 = other.value

      map2.foreach{
        case (cid, hc) => {
          val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = {
      hcMap
    }
  }
}
