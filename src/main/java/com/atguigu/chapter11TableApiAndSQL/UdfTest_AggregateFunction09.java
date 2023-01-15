package com.atguigu.chapter11TableApiAndSQL;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class UdfTest_AggregateFunction09 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 自定义数据源，从流转换
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 2. 将流转换成表
        Table eventTable = tableEnv.fromDataStream(eventStream,
                $("user"),
                $("url"),
                $("timestamp").as("ts"),
                $("rt").rowtime());
        tableEnv.createTemporaryView("EventTable", eventTable);

        // 3. 注册自定义表函数
        tableEnv.createTemporarySystemFunction("WeightedAverage", WeightedAverage.class);
    }
    public static class WeightedAvgAccumulator{
        public long sum = 0;
        public int count = 0;
    }

    public static class WeightedAverage extends AggregateFunction<Long, WeightedAvgAccumulator> {
        @Override
        public Long getValue(WeightedAvgAccumulator accumulator) {
            if (accumulator.count == 0){
                return null; // 防止除0
            }else {
                return accumulator.sum / accumulator.count;
            }
        }

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        public void accumulate(WeightedAvgAccumulator accumulator, Long iValue, Integer iWeight){
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }
    }
}
