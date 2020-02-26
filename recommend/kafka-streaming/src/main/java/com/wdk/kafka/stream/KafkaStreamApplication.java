package com.wdk.kafka.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * @Description:
 * kafkaStream 清洗日志
 * @Author:wang_dk
 * @Date:2020/2/26 0026 12:03
 * @Version: v1.0
 **/
public class KafkaStreamApplication {



    public static void main(String[] args) {

        String brokerList = "master:9092,slave1:9092,slave2:9092";
        String zookeeperList = "master:2181,salve1:2181,slave2:2181";

        String source = "log";  //输入topic
        String out = "recommender"; //输出topic

        Properties setting = new Properties();

        setting.put(StreamsConfig.APPLICATION_ID_CONFIG,"logFilter");
        setting.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,MyEventTimeExtractor.class);
        setting.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        setting.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,zookeeperList);

        StreamsConfig config = new StreamsConfig(setting);

        //构建拓扑器
        TopologyBuilder builder = new TopologyBuilder();

        //定义流处理的拓扑结构
        builder.addSource("SOURCE",source)  //输入
                .addProcessor("PROCESS",()-> new LogProcessor(),"SOURCE")   //从输入中接受数据 并调用LogProcessor.process()方法处理
                .addSink("SINK",out,"PROCESS");  //把处理结果输出

        //定义KafkaStrem
        KafkaStreams streams = new KafkaStreams(builder,config);

        //启动KafkStream
        streams.start();

        System.out.println("kafka stream started.......");
    }
}
