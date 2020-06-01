package com.wdk.flink.detect

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Description:
  *              统计1分钟内热门商品 排行top3
  * @Author:wang_dk
  * @Date:2020-06-01 21:17
  * @Version: v1.0
  **/
object PvHotItems {
    def main(args: Array[String]): Unit = {
        //定义Flink执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(4)

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //获取KafkaSource 消费recommender_pv topic
        val kafkaSource = FlinkKafkaSource.apply().getFlinkKafkaSources("recommender_pv")

        val pvStream = env.addSource(kafkaSource)
                .map(pvItem => {
                    //转换数据结构
                    val attr = pvItem.split("\\|")
                    PvItem(attr(0).trim.toInt, attr(1).trim, attr(2).trim.toLong)
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PvItem](Time.seconds(1)) {
            override def extractTimestamp(element: PvItem): Long = element.timeStamp * 1000
        })
                .keyBy(_.itemId)
                .timeWindow(Time.minutes(1), Time.seconds(20))
                .aggregate(new PvAggregateFunction(), new PvWindowFunction())
                //在根据窗口聚合
                .keyBy(_.windowEnd)
                .process(new PvKeyedProcessFunction())

        pvStream.print("PV>")

        env.execute("统计热门商品")
    }
}
//102383|pv|1590919401
case class PvItem(itemId:Int,eventType:String,timeStamp:Long)

case class WindowPvResult(itemId:Int,windowEnd:Long,nums:Long)

class PvAggregateFunction() extends AggregateFunction[PvItem,Long,Long]{
    override def add(value: PvItem, accumulator: Long) = accumulator+1

    override def createAccumulator() = 0L

    override def getResult(accumulator: Long) = accumulator

    override def merge(a: Long, b: Long) = a+b
}

class PvWindowFunction() extends WindowFunction[Long,WindowPvResult,Int,TimeWindow]{
    override def apply(key: Int, window: TimeWindow, input: Iterable[Long], out: Collector[WindowPvResult]): Unit = {
        out.collect(WindowPvResult(key,window.getEnd,input.iterator.next()))
    }
}

class PvKeyedProcessFunction() extends KeyedProcessFunction[Long,WindowPvResult,String]{
    //定义一个ListState 保留窗口数据
    var dataList : ListState[WindowPvResult] = null;


    override def open(parameters: Configuration) = {
        dataList = getRuntimeContext.getListState(new ListStateDescriptor[WindowPvResult]("itemState",classOf[WindowPvResult]))
    }

    override def processElement(value: WindowPvResult, ctx: KeyedProcessFunction[Long, WindowPvResult, String]#Context, out: Collector[String]) = {
        //窗口内每条数据进来 先放到dataList
        dataList.add(value)

        //注册一个定时器 窗口关闭的时候 统计结果
        ctx.timerService().registerEventTimeTimer(value.windowEnd+1000)
    }

    //定时器回调函数
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, WindowPvResult, String]#OnTimerContext, out: Collector[String]) = {
        //引入 scala 和java 的 集合转换器
        import scala.collection.JavaConversions._

        val items = dataList.get()

        val sortItems = items.toList    //itearable 转成List
                .sortBy(_.nums)(Ordering.Long.reverse)  //根据出现次数排序
                .take(3) //取top3

        //清空状态
        dataList.clear()

        //输出结果
        val result : StringBuilder = new StringBuilder
        result.append("时间:").append(new Timestamp(timestamp-1000)).append("\n")
        for(i <- sortItems.indices){  //根据sortedItems的下标遍历
            val item = sortItems(i)
            result.append("No"+(i+1) + ": 商品ID="+item.itemId+" 浏览量="+item.nums+"\n")
        }
        result.append("================================")

        //控制输出频率
        TimeUnit.SECONDS.sleep(2)

        out.collect(result.toString())
    }
}
