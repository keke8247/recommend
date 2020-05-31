package com.wdk.flume.etl;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Description:
 * 使用flume 对日志文件进行简单的清洗.
 * @Author:wang_dk
 * @Date:2020-05-31 15:11
 * @Version: v1.0
 **/
public class RecommendLogETLInterceptor implements Interceptor{
    @Override
    public void initialize() {

    }

    /**
     * @Description:
     * 对单挑日志做一个简单清洗.并根据前缀 放入到不同的kafkatopic
     * 登陆日志
     * LOGIN_PREFIX:96354|login|1590909556
     * 浏览日志
     * PRODUCT_PV_PREFIX:102383|pv|1590909302
     * 评分日志
     * PRODUCT_RATING_PREFIX:96354|102383|5.0|1590909303
     *
     * @Date 2020-05-31 15:22
     * @Param
     * @return
     **/
    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        Map<String, String> headers = event.getHeaders();

        if(log.contains("LOGIN_PREFIX:")){
            //获取格式化后的登录日志数据
            String loginLog = log.split("LOGIN_PREFIX:")[1].trim();
            headers.put("topic","recommender_login");
            event.setBody(loginLog.getBytes());
        }else if(log.contains("PRODUCT_RATING_PREFIX:")){
            //获取格式化后的评分日志数据
            String loginRating = log.split("PRODUCT_RATING_PREFIX:")[1].trim();
            headers.put("topic","recommender_rating");
            event.setBody(loginRating.getBytes());
        }else if(log.contains("PRODUCT_PV_PREFIX:")){
            //获取格式化后的浏览日志数据
            String loginPv = log.split("PRODUCT_PV_PREFIX:")[1].trim();
            headers.put("topic","recommender_pv");
            event.setBody(loginPv.getBytes());
        }else{
            return null;
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : list) {
            Event intercept1 = intercept(event);

            if (intercept1 != null){
                interceptors.add(intercept1);
            }
        }

        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new RecommendLogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
