package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

         //source
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        // 1. custom class
        SingleOutputStreamOperator<Event> result1 = stream.filter(new MyFilter());
        result1.print("1");

        // 2. from 匿名类
        SingleOutputStreamOperator<Event> result2 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Bob");
            }
        });
        result2.print("2");

        // 3. from lambda 表达式
        stream.filter(data -> data.user.equals("Bob")).print("lambda: Bob click");

        env.execute();

    }

    public static class MyFilter implements FilterFunction<Event>{
        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("Bob");
        }
    }
}
