package com.atguigu.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.loginfail_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/26 16:03
  */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取数据，准备 dataStream
    val resource = getClass.getResource("/LoginLog.csv")
//    val loginEventStream = env.readTextFile(resource.getPath)
    val loginEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(6)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.userId)

    // 2. 定义一个 pattern，用来检测 dataStream里的连续登录失败事件
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail")   // 第一次登录失败
      .next("secondFail").where(_.eventType == "fail")    // 第二次登录失败
      .within(Time.seconds(5))

    // 3. 将 pattern 应用于当前的数据流，得到一个 PatternStream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream, loginFailPattern)

    // 4. 定义一个 SelectFunction，从检测到匹配的复杂事件流中提取事件，输出报警信息
    val warningStream = patternStream.select( new LoginFailMatch() )

    warningStream.print()
    env.execute("login fail detect with cep job")
  }
}

// 自定义SelectFunction，输出连续登录失败的报警信息
class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning]{
  override def select(pattern: util.Map[String, util.List[LoginEvent]]): Warning = {
    // 从组合事件 map中提取匹配好的单个事件
    val firstFailEvent = pattern.get("firstFail").iterator().next()
    val secondFailEvent = pattern.get("secondFail").iterator().next()
    Warning( firstFailEvent.userId, firstFailEvent.eventTime, secondFailEvent.eventTime, "login fail" )
  }
}