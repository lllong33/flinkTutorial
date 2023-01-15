package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //source
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        // 1. 使用自定义类，实现 map 操作
        SingleOutputStreamOperator<String> result1 = stream.map(new MyMapper());

        // 2. 匿名类
        SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.user;
            }
        });

        // 3. lambda
        SingleOutputStreamOperator<String> result3 = stream.map(data -> data.user);

        result1.print("1");
        result2.print("2");
        result3.print("3");

        env.execute();
    }

    public static class MyMapper implements MapFunction<Event, String>{
        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    }
}
