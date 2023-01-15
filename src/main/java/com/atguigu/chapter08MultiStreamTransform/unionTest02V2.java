package com.atguigu.chapter08MultiStreamTransform;

import Util.DateTimeUtil;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class unionTest02V2 {
    // 两条数据类型一样的流union，验证水位线本质，以最小watermark推进
        // 改用socket流
        /*
        stream1
        Alice1, ./home, 1000
        Alice2, ./home, 3000

        stream2
        Alice2_1, ./home, 2000
        Alice2_2, ./home, 5000


        * */
    // 默认水位线好像是有序bound，时间戳呢？
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 大坑，如果这里并行度不是1的话，测试结果一直是最小值。

        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("node1", 7777)
                .map(data -> {
                    String[] field = data.split(",");
                    return new Event(field[0].trim(), field[1].trim(), Long.valueOf(field[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                System.out.println("stream1 wm: " + recordTimestamp);
                                return element.timestamp;
                            }
                        }))
                ;
        stream1.print("stream1>>>");

        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("node2", 7777)
                .map(data -> {
                    String[] field = data.split(",");
                    return new Event(field[0].trim(), field[1].trim(), Long.valueOf(field[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                System.out.println("stream2 wm: " + recordTimestamp);
                                return element.timestamp;
                            }
                        }))
                ;

        stream2.print("stream2>>>");

        stream1.union(stream2)
                .map(element -> {
                    element.datetime = DateTimeUtil.todtString(element.timestamp);
                    return element;
                })
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value.toString());
                        out.collect("now watermark: " + ctx.timerService().currentWatermark() + " fm: " + DateTimeUtil.todtString(ctx.timerService().currentWatermark()));
                    }
                })
                .print("unionStream>>>>");

        env.execute();


    }
}
