package com.atguigu.networkflow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
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
  * Created by wushengran on 2020/2/25 14:06
  */

// 定义样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
case class PvCount(windowEnd: Long, count: Long)

object PageView {
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
      .map( data => ("pv", 1) )    // map成所有数据拥有共同的key，然后统一做聚合
      .keyBy(_._1)
      .timeWindow(Time.hours(1))    // 滚动窗口，统计每个小时的pv量
      .aggregate(new PvCountAgg(), new PvResult())

    processedStream.print()
    env.execute("page view job")
  }
}

// 自定义预聚合函数
class PvCountAgg() extends AggregateFunction[(String, Int), Long, Long]{
  override def add(value: (String, Int), accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数
class PvResult() extends WindowFunction[Long, PvCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect( PvCount(window.getEnd, input.iterator.next()) )
  }
}
