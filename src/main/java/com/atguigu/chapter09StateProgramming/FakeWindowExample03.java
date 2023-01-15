package com.atguigu.chapter09StateProgramming;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// mapState, 使用 KeyedProcessFunction 模拟滚动窗口
// 结果：// url：urlValue 访问量: pv 窗口: start~end
public class FakeWindowExample03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.print("rawData:");

        stream.keyBy(f -> f.url)
                .process(new FakeWindowResult(10000L))
                .print(">>>");

        env.execute();
    }

    private static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {
        private Long windowSize;
        // 窗口start~end
        MapState<Long, Long> windowPvMapState;

        public FakeWindowResult(long twindowSize) {
            this.windowSize = twindowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            windowPvMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-pv", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 确定数据数据那个窗口
            Long windowStart = value.timestamp / windowSize * windowSize; //qst 这个规则待验证, 这里实现了一个功能，根据时间戳生成窗口段[10，20），[20, 30), [30, 40)；为什么不能是time/windowSize，没理解这里平方作用。
            Long windowEnd = windowStart + windowSize;

            ctx.timerService().registerEventTimeTimer(windowEnd - 1);

            if (windowPvMapState.contains(windowStart)){
                Long pv = windowPvMapState.get(windowStart);
                windowPvMapState.put(windowStart, pv + 1L);
            } else{
                windowPvMapState.put(windowStart, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;
            Long pv = windowPvMapState.get(windowStart);
            out.collect("url: " + ctx.getCurrentKey() +
                    "; pageView: " + pv +
                    " windowRange:" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd));
        }
    }
}
