package com.atguigu.chapter08MultiStreamTransform;

import Util.DateTimeUtil;
import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class unionTest02 {
    // 两条数据类型一样的流union，验证水位线本质，以最小watermark推进
        // 使用自定义元素好像很难验证，unionStream waterMark改变时，为stream2第二数据达到的第一条watermark
    // 默认水位线好像是有序bound，时间戳呢？
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 大坑，如果这里并行度不是1的话，测试结果一直是最小值。

        SingleOutputStreamOperator<Event> stream1 = env.fromElements(
                new Event("Alice1", "./home", 10000L),
                new Event("Alice2", "./cart", 15000L)

        )
                .map(element -> {
                    element.datetime = DateTimeUtil.todtString(element.timestamp);
                    return element;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }))
                ;
        stream1.print("stream1>>>");

        SingleOutputStreamOperator<Event> stream2 = env.fromElements(
                new Event("Alice2_1", "./home", 12000L),
                new Event("Alice2_2", "./cart", 30000L)
        )
                .map(element -> { // map放到wm后面，速度会慢很多
                    element.datetime = DateTimeUtil.todtString(element.timestamp);
                    return element;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
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
