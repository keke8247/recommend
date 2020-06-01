package com.wdk.flink.detect

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * @Description:
  *              登录监控. 1分钟内连续登录3次 冻结账号.
  * @Author:wang_dk
  * @Date:2020-05-31 20:12
  * @Version: v1.0
  **/
object LoginDetect {
    def main(args: Array[String]): Unit = {
        //构建Flink执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)

        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //声明consumer
        val sourceKafka = FlinkKafkaSource.apply().getFlinkKafkaSources("recommender_login")

        val logStream = env.addSource(sourceKafka).map(log=>{
            val attr = log.split("\\|")
            LoginMsg(attr(0).trim.toLong,attr(1).trim,attr(2).trim.toLong)
        }).assignAscendingTimestamps(_.timeStamp)

        val warningStream = logStream.keyBy(_.userId).process(new LoginMonitorProcess(3,30))

        warningStream.print("warning>")

        env.execute("Flink 监控登录信息")

    }

}

class LoginMonitorProcess(maxLoginNums: Int, timeStep: Int) extends KeyedProcessFunction[Long,LoginMsg,LoginLimit]{
    //声明一个状态 记录登录次数
    lazy val numsState : ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("loginNums",classOf[Int]))
    override def processElement(value: LoginMsg, ctx: KeyedProcessFunction[Long, LoginMsg, LoginLimit]#Context, out: Collector[LoginLimit]) = {
        if(numsState.value() == 0){ //第一次登录
            numsState.update(1)

            //注册一个定时器
            ctx.timerService().registerEventTimeTimer(value.timeStamp+timeStep*1000)
        }else{
            numsState.update(numsState.value()+1)
        }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginMsg, LoginLimit]#OnTimerContext, out: Collector[LoginLimit]) = {
        if(numsState.value()>=maxLoginNums){
            out.collect(LoginLimit(ctx.getCurrentKey,numsState.value(),"连续登录次数过多.限制登录5分钟后重试."))
        }
        numsState.clear()
    }
}

case class LoginMsg(userId:Long,eventType:String,timeStamp:Long)

case class LoginLimit(userId:Long,loginNums:Int,msg:String)
