package com.gaogzhen.bigdata.spark.core.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import org.apache.kafka.common.serialization.StringSerializer

object SparkKafkaProducer {
  def main(args: Array[String]): Unit = {
    // 1 kafka配置信息
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    // 2 创建kafka生产者
    val producer = new KafkaProducer[String, String](properties)
    // 3 发送数据
    for (i <- 1 to 5) {
      producer.send(new ProducerRecord[String, String]("first", "spark" + i))
    }

    // 4关闭资源
    producer.close();
  }
}