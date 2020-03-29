package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/25 14:26
  */

case class UvCount(windowEnd: Long, count: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 开窗聚合
    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))    // 滚动窗口，统计每个小时的pv量
      .apply( new UvCountByWindow() )

    processedStream.print()
    env.execute("unique visitor job")
  }
}

// 自定义全窗口函数
class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 用一个set类型来保存所有的userId，做到自动去重
    var idSet = Set[Long]()
    // 遍历窗口所有数据，全部放入set中
    for( userBehavior <- input ){
      idSet += userBehavior.userId
    }
    // 输出 UvCount 统计结果
    out.collect( UvCount(window.getEnd, idSet.size) )
  }
}