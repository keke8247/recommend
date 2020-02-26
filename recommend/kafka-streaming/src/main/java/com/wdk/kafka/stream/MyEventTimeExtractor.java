package com.wdk.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/2/26 0026 16:30
 * @Version: v1.0
 **/

public class MyEventTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        return System.currentTimeMillis()/1000;
    }
}
