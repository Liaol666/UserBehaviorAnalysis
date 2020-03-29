package com.atguigu.orderpay_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/28 10:35
  */

// 输入数据的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)
// 输出订单检测结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 0. 从文件中读取数据
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(0)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.orderId)

    // 1. 定义一个带时间限制的模式
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 2. 在orderEventStream上应用pattern，生成一个PatternStream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // 3. 定义一个侧输出流标签，用于把 timeout事件输出到侧输出流里去
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")
    // 4. 调用select方法，得到最终的输出结果
    val resultStream = patternStream.select( orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    env.execute("order timeout detect job")
  }
}

// 自定义一个PatternTimeoutFunction
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val timeoutOrderId = pattern.get("create").iterator().next().orderId
    OrderResult( timeoutOrderId, "timeout at " + timeoutTimestamp )
  }
}

// 自定义一个 PatternSelectFunction
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = pattern.get("pay").iterator().next().orderId
    OrderResult( payedOrderId, "payed successfully" )
  }
}