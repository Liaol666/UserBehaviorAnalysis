package com.atguigu.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.loginfail_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/26 11:47
  */

// 输入登录事件样例类
case class LoginEvent( userId: Long, ip: String, eventType: String, eventTime: Long )
// 输出报警信息样例类
case class Warning( userId: Long, firstFailTime: Long, lastFailTime: Long, msg: String )

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(6)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    // 检测2s内连续登录失败，报警
    val warningStream = loginEventStream
      .keyBy(_.userId)
      .process( new LoginWarning(2) )

    warningStream.print()
    env.execute("login fail detect job")
  }
}

// 自定义实现 ProcessFunction，将之前的登录失败事件存入状态，注册定时器，2s后判断有几次登录失败
class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning]{
  // 定义一个list state，用于保存已经到达的连续登录失败事件
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(
    new ListStateDescriptor[LoginEvent]("saved-loginfails", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 判断事件类型，只添加登录失败事件，如果是成功事件，直接清空状态
    if( value.eventType == "fail" ){
//      loginFailListState.add(value)
//      // 注册定时器，设定2秒后触发
//      ctx.timerService().registerEventTimeTimer( value.eventTime * 1000L + 2000L )
      // 改进实现：判断之前是否已有登录失败事件，如果有，判断时间戳差值；如果没有，把当前第一个失败事件存入状态
      val iter = loginFailListState.get().iterator()
      if( iter.hasNext ){
        // 如果已经有一次登录失败了，取出，跟当前登录失败取差值
        val firstFailEvent = iter.next()
        if( (firstFailEvent.eventTime - value.eventTime).abs < 2 ){
          // 如果在两秒以内，输出报警信息
          out.collect( Warning(value.userId, firstFailEvent.eventTime, value.eventTime, "login fail for 2 times") )
        }
        // 如果当前时间戳大于上次登录失败，更新最近的登录失败事件
        if( value.eventTime > firstFailEvent.eventTime ){
          loginFailListState.clear()
          loginFailListState.add(value)
        }
      } else{
        // 如果之前没有失败事件，那么当前失败事件是第一个，直接添加到状态
        loginFailListState.add(value)
      }
    } else{
      // 如果中间遇到了成功事件，清空状态重新开始
      loginFailListState.clear()
    }
  }

//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
//    import scala.collection.JavaConversions._
//    // 先从liststate中取出所有的登录失败事件
//    val allLoginFails = loginFailListState.get().iterator().toList
//    // 判断总共的失败事件是否超过 maxFailTimes
//    if( allLoginFails.length >= maxFailTimes ){
//      // 输出报警信息
//      out.collect( Warning(allLoginFails.head.userId,
//        allLoginFails.head.eventTime,
//        allLoginFails.last.eventTime,
//        "login fail in 2s for " + allLoginFails.length + " times") )
//    }
//    // 清空状态
//    loginFailListState.clear()
//  }
}