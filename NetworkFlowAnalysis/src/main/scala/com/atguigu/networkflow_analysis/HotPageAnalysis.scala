package com.atguigu.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/25 9:23
  */
// 定义一个输入数据的样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

// 定义中间的聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object HotPageAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
    val resource = getClass.getResource("/apache.log")
    //    val dataStream = env.readTextFile(resource.getPath)
    val dataStream = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray = data.split(" ")
        // 对事件时间进行转换，得到Long的时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3)).getTime
        ApacheLogEvent(dataArray(0), dataArray(1), timestamp, dataArray(5), dataArray(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })

    // 开窗聚合
    val aggStream = dataStream
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(10))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      .aggregate(new UrlCountAgg(), new UrlCountResult())

    // 排序输出
    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    resultStream.print("result")
    dataStream.print("input")
    aggStream.print("agg")
    aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")
    env.execute("hot page job")
  }
}

// 自定义预聚合函数
class UrlCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数，包装成样例类
class UrlCountResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

// 自定义实现Process Function，先保存列表状态，然后定时排序输出
class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  // 先定义一个list状态，用来保存所有的聚合结果
  //  lazy val urlListState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-list", classOf[UrlViewCount]))
  // 改进：用 MapState (url, count)来保存聚合结果，用于迟到数据更新聚合结果时去重
  lazy val urlMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("url-map", classOf[String], classOf[Long]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每条数据直接放入list state
    urlMapState.put(value.url, value.count)
    // 定义一个 windowEnd + 1 的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 定时器触发时，watermark涨过了windowEnd，所以所有聚合结果都到期了
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 从状态获取所有的聚合结果
    //    val allUrlsCount: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()
    //    val iter = urlListState.get().iterator()
    //    while(iter.hasNext)
    //      allUrlsCount += iter.next()
    val allUrlsCount: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]()
    val iter = urlMapState.entries().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      allUrlsCount += ((entry.getKey, entry.getValue))
    }

//    urlListState.clear()

    // 排序输出
    val sortedUrlsCount = allUrlsCount.sortWith(_._2 > _._2).take(topSize)

    // 格式化输出
    val result: StringBuilder = new StringBuilder()
    result.append("==================================\n")
    result.append("窗口关闭时间：").append(new Timestamp(timestamp - 1)).append("\n")
    // 用一个for循环遍历sortedList，输出前三名的所有信息
    for (i <- sortedUrlsCount.indices) {
      val currentUrl = sortedUrlsCount(i)
      result.append("No").append(i + 1).append(":")
        .append(" URL=").append(currentUrl._1)
        .append(" 访问量=").append(currentUrl._2)
        .append("\n")
    }
    // 控制显示频率
    Thread.sleep(1000L)
    // 最终输出结果
    out.collect(result.toString())
  }
}