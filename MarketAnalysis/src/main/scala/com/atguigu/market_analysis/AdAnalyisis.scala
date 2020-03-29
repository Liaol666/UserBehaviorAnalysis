package com.atguigu.market_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.{AggregateFunction, RichFilterFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.market_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/26 9:19
  */

// 输入log数据样例类
case class AdClickLog( userId: Long, adId: Long, province: String, city: String, timestamp: Long)
// 输出按省份分类统计结果样例类
case class AdCountByProvince( windowEnd: String, province: String, count: Long)
// 定义侧输出流黑名单报警信息的样例类
case class BlackListWarning( userId: Long, adId: Long, msg: String )

object AdAnalyisis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据
    val resource = getClass.getResource("/AdClickLog.csv")
    val adLogStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 黑名单过滤，侧输出流报警
    val filterBlackListStream = adLogStream
      .keyBy( data => (data.userId, data.adId) )
//    .filter(new MyFilter(100))
      .process( new FilterBlackListUser(100) )

    // 开窗聚合
    val adCountStream = filterBlackListStream
      .keyBy(_.province)
      .timeWindow( Time.hours(1), Time.seconds(5) )
      .aggregate( new AdCountAgg(), new AdCountResult() )

    adCountStream.print("count")
    filterBlackListStream.getSideOutput(new OutputTag[BlackListWarning]("blacklist")).print("black-list")
    env.execute("ad analysis job")
  }
}

// 自定义预聚合函数
class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long]{
  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
// 自定义窗口函数
class AdCountResult() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
    val end = new Timestamp( window.getEnd ).toString
    out.collect(AdCountByProvince(end, key, input.iterator.next()))
  }
}

// 自定义KeyedProcessFunction
class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]{
  // 定义状态，用来保存当前用户对当前广告的点击量
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  // 定义状态，标记当前用户是否已经进入黑名单
  lazy val isSentState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-sent", classOf[Boolean]))
  // 定义状态，保存0点触发的定时器时间戳
  lazy val resetTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-timer", classOf[Long]))

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    // 从状态中取出count值
    val curCount = countState.value()
    // 如果是第一次点击，那么注册定时器，第二天0点触发，清除状态
    if( curCount == 0 ){
      val ts = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (1000*60*60*24)
      ctx.timerService().registerProcessingTimeTimer(ts)
      resetTimerState.update(ts)
    }
    // 判断如果当前count超过了上限，那么加入黑名单，侧输出流输出报警
    if( curCount >= maxCount ){
      // 判断是否发到过黑名单，如果已经发过就不用了
      if( ! isSentState.value() ){
        ctx.output(new OutputTag[BlackListWarning]("blacklist"), BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times"))
        isSentState.update(true)
      }
    } else {
      // 如果没达到上限，count + 1，value正常进入主流
      countState.update(curCount + 1)
      out.collect( value )
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    // 如果是0点的时间戳，那么触发清空操作
    if( timestamp == resetTimerState.value() ){
      isSentState.clear()
      countState.clear()
      resetTimerState.clear()
    }
  }
}

class MyFilter(maxCount: Int) extends RichFilterFunction[AdClickLog]{
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))

  override def filter(value: AdClickLog): Boolean = {
    val curCount = countState.value()
    if( curCount >= maxCount){
      false
    } else {
      countState.update( curCount + 1 )
      true
    }
  }

  override def close(): Unit = {
    countState.clear()
  }
}