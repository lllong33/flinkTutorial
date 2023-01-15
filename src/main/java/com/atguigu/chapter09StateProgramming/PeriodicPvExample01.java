package com.atguigu.chapter09StateProgramming;


import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

// valueState example
// 统计每个用户的pv，隔一段时间（10s）输出一次结果
// countState 用于累计，timerTsState判断是否触发
public class PeriodicPvExample01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        stream.print("RawData>>>>>>>");

        // pv
        stream.keyBy(f -> f.user)
                .process(new pvProcess())
                .print("pv>>>>>>>");

        env.execute();

    }

    private static class pvProcess extends KeyedProcessFunction<String, Event, String> {
        ValueState<Long> countState;
        ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState", Long.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState", Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 累计
            if (countState.value() != null){
                countState.update(countState.value() + 1L);
            } else {
                countState.update(1L);
            }

            // 注册定时器
            if (timerState.value() == null){
                Long timer = value.timestamp + 10 * 1000L;
                ctx.timerService().registerEventTimeTimer(timer);
                timerState.update(timer);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + " pv: " + countState.value());
            timerState.clear();
        }
    }
}
