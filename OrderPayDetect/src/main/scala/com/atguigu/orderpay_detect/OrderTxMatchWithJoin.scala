package com.atguigu.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/28 15:49
  */
object OrderTxMatchWithJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据，来自Order和Receipt两条流
    val orderPayResource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(orderPayResource.getPath)
//    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(0)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })
      .filter( _.txId != "" )    // 过滤出txId不为空的订单支付事件
      .keyBy(_.txId)    // 用交易号分组进行两条流的匹配

    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(receiptResource.getPath)
//    val receiptEventStream = env.socketTextStream("localhost", 8888)
      .map( data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(0)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.txId)    // 用交易号分组进行两条流的匹配

    // join合流并处理
    val processedStream = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-3), Time.seconds(5))
      .process( new TxPayMatchByJoin() )

    processedStream.print("matched")
    env.execute("order tx match")
  }
}

// 自定义 ProcessJoinFunction
class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect( (left, right) )
  }
}