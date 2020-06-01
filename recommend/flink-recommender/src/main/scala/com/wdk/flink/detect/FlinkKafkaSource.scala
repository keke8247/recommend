package com.wdk.flink.detect

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020-06-01 20:21
  * @Version: v1.0
  **/
case class FlinkKafkaSource() {
    def getFlinkKafkaSources(topic:String) : FlinkKafkaConsumer[String] ={
        //构建KafkaSource
        //从kafka消费数据
        val properties = new Properties()
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092,slave2:9092")
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"flink-recommender")
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")

        new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),properties)
    }
}
