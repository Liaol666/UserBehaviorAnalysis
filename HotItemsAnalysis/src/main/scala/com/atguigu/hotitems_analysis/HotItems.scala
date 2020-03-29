package com.atguigu.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.hotitems_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/24 14:02
  */

// 定义输入数据样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
// 定义中间聚合结果样例类
case class ItemViewCount( itemId: Long, windowEnd: Long, count: Long )

object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取数据
//    val dataStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 2. 开窗聚合数据
    val aggregatedStream = dataStream
      .filter(_.behavior == "pv")    // 过滤出pv行为
      .keyBy("itemId")    // 按照 itemId分组
      .timeWindow(Time.hours(1), Time.minutes(5))    // 开滑动窗口
//      .aggregate(new AverageAgg)
      .aggregate(new CountAgg(), new WindowResult())    // 窗口聚合操作

    // 3. 排序输出 TopN
    val resultStream = aggregatedStream
      .keyBy("windowEnd")    // 按照窗口分组
      .process( new TopNItems(3) )    // 自定义 process function做排序输出

    resultStream.print()
    env.execute()
  }
}

// 自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{
  // 每来一条数据就加 1
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
// 另一个例子，计算平均数(数据时间戳的平均值)
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double]{
  // 每来一条数据，把时间戳叠加在状态1中，把状态2的个数加1
  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) =
    ( accumulator._1 + value.timestamp, accumulator._2 + 1 )

  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2.toDouble

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = ( a._1 + b._1, a._2 + b._2 )
}

class WindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd: Long = window.getEnd
    val count: Long = input.iterator.next()
    out.collect( ItemViewCount(itemId, windowEnd, count) )
  }
}

// 自定义 process function
class TopNItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{
  // 先定义一个列表状态，用来保存当前窗口中所有 item的count聚合结果
  private var itemListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-list", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每条数据来了之后保存入状态，第一条数据来的时候，注册一个定时器，延迟 1毫秒
    itemListState.add(value)
    // 可以直接每个数据都注册一个定时器，因为定时器是以时间戳作为id的，同样时间戳的定时器不会重复注册和触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 等到watermark超过windowEnd，达到windowEnd+1，说明所有聚合结果都到齐了，排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
//    val allItemsList: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
//    for( item <- itemListState.get()){
//      allItemsList += item
//    }
    // 1. 定义一个本地的list，用于提取所有的聚合的值，方便后面做排序
    val allItemsList = itemListState.get().iterator().toList
    // 清空list state，释放资源
    itemListState.clear()

    // 2. 按照count大小进行排序
    val sortedItemsList = allItemsList.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 3. 将排序后列表中的信息，格式化之后输出
    val result: StringBuilder = new StringBuilder()
    result.append("==================================\n")
    result.append("窗口关闭时间：").append( new Timestamp(timestamp - 1) ).append("\n")
    // 用一个for循环遍历sortedList，输出前三名的所有信息
    for( i <- sortedItemsList.indices){
      val currentItem = sortedItemsList(i)
      result.append("No").append( i + 1 ).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }
    // 控制显示频率
    Thread.sleep(1000L)
    // 最终输出结果
    out.collect(result.toString())
  }
}