package com.atguigu.chapter07ProcessFunction;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;


/*
8> data arrive, the timestamp for: 1000
8> data arrive, the watermark for: -9223372036854775808
 -------split line------------
8> data arrive, the timestamp for: 11000
8> data arrive, the watermark for: 999   // 这里水位线慢是周期性触发原因
 -------split line------------
8> data arrive, the timestamp for: 11001
8> data arrive, the watermark for: 10999
 -------split line------------
8> timer trigger, time is: 11000
8> timer trigger, time is: 21000   // 这里为流停止，水位线到达最大值触发。
8> timer trigger, time is: 21001
* */
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("data arrive, the timestamp for: " + ctx.timestamp());
                        out.collect("data arrive, the watermark for: " + ctx.timerService().currentWatermark() + "\n -------split line------------");
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("timer trigger, time is: " + timestamp);
                    }
                })
                .print();

        env.execute();
    }

    public static class CustomSource implements SourceFunction<Event>{
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // 直接发出测试数据
            ctx.collect(new Event("Mary", "./home", 1000L));
            // 为了更加明显，中间停顿5秒钟
            Thread.sleep(5000L);

            // 发出10秒后的数据
            ctx.collect(new Event("Mary", "./home", 11000L));
            Thread.sleep(5000L);

            // 发出10秒+1ms后的数据
            ctx.collect(new Event("Alice", "./cart", 11001L));
            Thread.sleep(5000L);
        }

        @Override
        public void cancel() { }
    }
}
