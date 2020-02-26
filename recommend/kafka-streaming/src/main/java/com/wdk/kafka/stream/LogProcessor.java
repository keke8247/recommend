package com.wdk.kafka.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/2/26 0026 12:13
 * @Version: v1.0
 **/

public class LogProcessor implements Processor<byte[],byte[]>{
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    /**
     * @Description:
     * 执行数据清洗工作的方法
     * @Date 2020/2/26 0026 12:15
     * @Param dummy: 数据的Key
     *          line: 真正要处理的数据
     * @return
     **/
    @Override
    public void process(byte[] dummy, byte[] line) {
        String logLine = new String(line);
        if(logLine.contains("PRODUCT_RATING_PREFIX:")){
            System.out.println("pruduct rating comming!!! "+ logLine);
            logLine = logLine.split("PRODUCT_RATING_PREFIX:")[1].trim();    //得到格式化的评分数据
            System.out.println("~~~~~~~~~~~~~~~~~"+logLine);
            //写回到kafka
            context.forward("logProcessor".getBytes(),logLine.getBytes());
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
