package com.atguigu.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.hotitems_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/24 16:30
  */
object KafkaProducerTest {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
  def writeToKafka(topic: String): Unit ={
    // 定义配置参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    // 创建一个Kafka producer
    val producer = new KafkaProducer[String, String](properties)
    // 从文件中读取测试数据，逐条发送
    val bufferedSource = io.Source.fromFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for( line <- bufferedSource.getLines() ){
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
