package com.atguigu.chapter05;


import java.sql.Timestamp;

// POJO Event 类
public class Event {
    // 定义共有属性且可序列化 user, url, timestamp
    public String user;
    public String url;
    public Long timestamp;
    public String datetime;

    // 无参构建方法
    public Event(){

    }

    //
    public Event(String user, String url, Long timestamp){
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    // 重写 toString 方法

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                ", datetime=" + datetime +
                '}';
    }
}
