package com.atguigu.chapter09StateProgramming;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// AggregatingState
// 统计每个用户的点击频次，到达5次就输出统计结果
// qst这里业务含义好奇怪，计算的是中间时间点，无法推出起始和终点时间？
public class AverageTimestampExample04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
//                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
                    .withTimestampAssigner( (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        stream.print("RawData>>>");

        stream.keyBy(f -> f.user)
                .flatMap(new AvgTsResult())
                .print("avgTimestamp>>>");

        env.execute();
    }

    private static class AvgTsResult extends RichFlatMapFunction<Event, String> {

        AggregatingState<Event, Long> avgTsAggState; // 计算平均时间
        ValueState<Long> countState;  // 每五次判断条件

        @Override
        public void open(Configuration parameters) throws Exception {
            avgTsAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                    "avgTsAggState",
                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1L);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            // 窗口内平均点击频率
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                            return null;
                        }
                    }
                    ,Types.TUPLE(Types.POJO(Event.class), Types.LONG)
            ));
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState", Long.class));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            Long count = countState.value();
            if (count != null){
                count ++;
            } else {
                count = 1L;
            }
            countState.update(count);
            avgTsAggState.add(value);

            // condition
            if (count == 5){
                out.collect(value.user + " avgTimestamp: " + new Timestamp(avgTsAggState.get()));
                countState.clear(); // avgTsAggState不清理，滚动统计
            }
        }
    }
}
