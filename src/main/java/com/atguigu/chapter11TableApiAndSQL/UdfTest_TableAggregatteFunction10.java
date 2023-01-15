package com.atguigu.chapter11TableApiAndSQL;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
//import com.sun.deploy.net.proxy.WFirefoxProxyConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

// topN 案例
public class UdfTest_TableAggregatteFunction10 {
    public static void main(String[] args) throws Exception {
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
        tableEnv.createTemporaryView("eventTable", eventTable);

        // 3. 开滚动窗口聚合，得到每个用户在每个窗口中的浏览量
        /*
        select user, count(url) as cnt,
            window_end
        from table(
            tumble(table eventTable, descriptor(rt), interval '10' second)
        ) group by user, window_start, window_end
        * */
        Table windwoAggTable = tableEnv.sqlQuery("select user, count(url) as cnt,\n" +
                "            window_end\n" +
                "        from table(\n" +
                "            tumble(table eventTable, descriptor(rt), interval '10' second)\n" +
                "        ) group by user, window_start, window_end");

        tableEnv.createTemporaryView("AggTable", windwoAggTable);

        tableEnv.createTemporarySystemFunction("Top2", Top2.class);

        // Table API invoke
        Table rstTable = tableEnv.from("AggTable")
                .groupBy($("window_end"))
                .flatAggregate(
                        call("Top2", $("cnt")).as("value", "rank")
                )
                .select($("window_end"), $("value"), $("rank"));
        tableEnv.toChangelogStream(rstTable).print();

        env.execute();


    }
    public static class Top2Accumulator{
        public long first;
        public long second;
    }

    public static class Top2 extends TableAggregateFunction<Tuple2<Long, Integer>, Top2Accumulator>{

        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            acc.first = Long.MIN_VALUE;
            acc.second = Long.MIN_VALUE;
            return acc;
        }

        public void accumulate(Top2Accumulator acc, Long value){
            if(value > acc.first){
                acc.first = value;
                acc.second = acc.first;
            } else if (value > acc.second){
                acc.second = value;
            }
        }

        public void emitValue(Top2Accumulator acc, Collector<Tuple2<Long, Integer>> out){
            if (acc.first != Long.MIN_VALUE){
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Long.MIN_VALUE){
                out.collect(Tuple2.of(acc.second, 2));
            }
        }

    }
}
