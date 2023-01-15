package com.atguigu.chapter08MultiStreamTransform;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
//import sun.plugin.liveconnect.OriginNotAllowedException;

public class WindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, String, Long>> stream1 = env
                .fromElements(
                        Tuple3.of("Stream1", "a", 1000L),
                        Tuple3.of("Stream1", "b", 1000L),
                        Tuple3.of("Stream1", "a", 5000L),
                        Tuple3.of("Stream1", "b", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple3<String, String, Long> stringLongTuple3, long l) {
                                                return stringLongTuple3.f2;
                                            }
                                        }
                                )
                );

        DataStream<Tuple3<String, String, Long>> stream2 = env
                .fromElements(
                        Tuple3.of("Stream2", "a", 3000L),
                        Tuple3.of("Stream2", "b", 4000L),
                        Tuple3.of("Stream2", "b", 5000L),
                        Tuple3.of("Stream2", "a", 6000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple3<String, String, Long> stringLongTuple3, long l) {
                                                return stringLongTuple3.f2;
                                            }
                                        }
                                )
                );

        stream1.join(stream2)
                .where(f -> f.f1)
                .equalTo(f -> f.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public String join(Tuple3<String, String, Long> first, Tuple3<String, String, Long> second) throws Exception {
                        return first + " => " + second;
                    }
                })
                .print();

        env.execute();
    }
}
