package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import scala.util.hashing.MurmurHash3

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/25 15:15
  */
object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 开窗聚合
    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .map(data => ("uv", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1)) // 滚动窗口，统计每个小时的pv量
        .trigger( new MyTrigger() )
      .process( new UvCountWithBlomm() )

    processedStream.print()
    env.execute("unique visitor with bloom job")
  }
}

// 自定义触发器
class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  // 每一个数据来了之后，都触发一次窗口计算操作
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }
}

// 自定义 process window function
class UvCountWithBlomm() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{
  // 创建 redis连接和布隆过滤器
  lazy val jedis = new Jedis("localhost", 6379)
  // 2^29，对应数据容量 512M，在redis中存储大小2^29bit,2^26Byte,64M
  lazy val bloom = new Bloom(1<<29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 在redis里存储的位图，每个窗口都以windowEnd作为位图的 key
    val storeKey = context.window.getEnd.toString
    // 定义当前窗口的uv count值，count值存在redis里，用hashmap存储(表名count)
    var count = 0L
    if( jedis.hget("count", storeKey) != null ){
      count = jedis.hget("count", storeKey).toLong
    }

    // 对userId取hash值得到偏移量，查看bitmap中是否存在
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)

    // 用redis命令查询bitmap
    val isExist = jedis.getbit(storeKey, offset)
    if(!isExist){
      // 如果不存在，那么将对应位置置1，然后count + 1
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      // 输出UvCount
      out.collect( UvCount(storeKey.toLong, count + 1) )
    }
  }
}

// 自定义布隆过滤器
class Bloom(size: Long) extends Serializable {
  // 容量是2的 n次方
  private val cap = size

  def hash(value: String, seed: Int): Long = {
    var result = 0
    for( i <- 0 until value.length ){
      result = result * seed + value.charAt(i)
    }
    // 返回hash，需要在cap范围内
    (cap - 1) & result
  }
}