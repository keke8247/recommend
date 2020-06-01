package com.wdk.flink.detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020-06-01 20:17
  * @Version: v1.0
  **/
object LoginDetectWithCEP {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(2)

        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //获取KafkaSource
        val flinkKafkaSource = FlinkKafkaSource.apply().getFlinkKafkaSources("recommender_login")

        val loginStream = env.addSource(flinkKafkaSource).map(log=>{
            val attr = log.split("\\|")
            LoginMsg(attr(0).trim.toLong,attr(1).trim,attr(2).trim.toLong)
        }).assignAscendingTimestamps(_.timeStamp).keyBy(_.userId)

        //定义连续登录pattern
        val continuousLoginPattern = Pattern.begin[LoginMsg]("begin").where(_.eventType=="login")
                .next("next").where(_.eventType == "login").within(Time.seconds(20)).times(2,4)

        //使用pattern
        val patternStream = CEP.pattern(loginStream,continuousLoginPattern)

        val continuousLoginStream = patternStream.select(new LoginMatch())

        continuousLoginStream.print()

        env.execute("使用CEP监控登录....")
    }
}

case class LoginMatch() extends PatternSelectFunction[LoginMsg,LoginLimit] {
    override def select(map: util.Map[String, util.List[LoginMsg]]) = {
        val loginList = new util.ArrayList[LoginMsg]
        val firstLogin = map.get("begin").iterator()

        while (firstLogin.hasNext){
            loginList.add(firstLogin.next())
        }

        val lastLogin = map.get("next").iterator()
        while (lastLogin.hasNext){
            loginList.add(lastLogin.next())
        }

        LoginLimit(loginList.get(0).userId,loginList.size(),"登录次数过多.....")
    }
}
