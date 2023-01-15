package com.atguigu.chapter09StateProgramming;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

// ListState
// 实现fulljoin，可以通过ListState来处理；qst这里可能存在状态过大问题
// 也可以通过 Window CoGroup 处理，存在窗口的概念。
public class TwoStreamFullJoinExample02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // key connect

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("a", "stream-1", 1000L),
                Tuple3.of("b", "stream-1", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
                Tuple3.of("a", "stream-2", 3000L),
                Tuple3.of("b", "stream-2", 4000L),
                Tuple3.of("a", "stream-2", 5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));

        // join
        stream1.keyBy(f -> f.f0)
                .connect(stream2.keyBy(f -> f.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    // ListState 分别记录历史数据，用于当前数据full join使用
                    ListState<Tuple3<String, String, Long>> stream1ListState;
                    ListState<Tuple3<String, String, Long>> stream2ListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        stream1ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream1ListState", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
                        stream2ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream2ListState", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, Context ctx, Collector<String> out) throws Exception {
                        stream1ListState.add(left);
//                        System.out.println("stream2: "+stream2ListState.get());
                        for(Tuple3<String, String, Long> right: stream2ListState.get()){
                            out.collect("leftJoin :"+ left + "=> " + right);
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
                        stream2ListState.add(right);
//                        System.out.println("stream1: "+stream1ListState.get());
                        for(Tuple3<String, String, Long> left: stream1ListState.get()){
                            out.collect("rightJoin :" + left + "=> " + right);
                        }
                    }
                })
                .print("fullJoin>>>>");

        env.execute();

    }
}
