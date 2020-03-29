package com.atguigu.market_analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.market_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/25 16:35
  */

// 定义输入数据样例类
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)
// 输出样例类
case class MarketViewCountByChannel(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

// 自定义数据源
class SimulatedDataSource() extends RichParallelSourceFunction[MarketUserBehavior]{
  // 是否运行的标识位
  var running = true
  // 定义出推广渠道和用户行为的集合
  val channelSet: Seq[String] = Seq("AppStore", "HuaweiStore", "XiaoStore", "weibo", "wechat")
  val behaviorSet: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  // 定义一个随机数生成器
  val rand: Random = new Random()

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    val maxElements = Long.MaxValue
    var count = 0L
    // 无限循环，生成随机数据
    while( running && count < maxElements ){
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()
      ctx.collect(MarketUserBehavior(id, behavior, channel, ts))
      count += 1
      Thread.sleep(10L)
    }
  }
}

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 生成数据
    val dataStream = env.addSource(new SimulatedDataSource())
      .assignAscendingTimestamps(_.timestamp)
    // 开窗聚合处理
    val processedStream = dataStream
      .filter(_.behavior != "UNINSTALL")
      .keyBy( data => (data.channel, data.behavior) )
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process( new MarketCountByChannel() )

    processedStream.print()
    env.execute("app market job")
  }
}

// 自定义的 Process Window Function
class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketViewCountByChannel, (String, String), TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCountByChannel]): Unit = {
    // 从上下文中获取window信息，包装成样例类
    val start = new Timestamp( context.window.getStart ).toString
    val end = new Timestamp( context.window.getEnd ).toString
    out.collect( MarketViewCountByChannel(start, end, key._1, key._2, elements.size) )
  }
}