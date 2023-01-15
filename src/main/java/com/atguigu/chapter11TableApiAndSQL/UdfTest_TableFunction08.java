package com.atguigu.chapter11TableApiAndSQL;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class UdfTest_TableFunction08 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        // 创建tabEnv, 流转表指定WM，注册表和UDF，调用UDF，输出到外部系统，获取TableFunction的结果
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table eventTable = tableEnv.fromDataStream(eventStream, $("user"), $("url"), $("timestamp").rowtime().as("ts"));
        tableEnv.createTemporaryView("eventTable", eventTable);
        tableEnv.createTemporarySystemFunction("mySplit", MySplit.class);
        /*
        select user, url, word, length
        from eventTable, lateral table( mySplit(url) ) as T(word, length)
        * */
        Table rst = tableEnv.sqlQuery("select user, url, word, length from eventTable, lateral table( mySplit(url) ) as T(word, length)");

        /*
        create table output (
            user string,
            url string,
            word string,
            length int
         ) with (
            'connector' = 'print'
         )

        * */
        tableEnv.executeSql("create table output (\n" +
                "            `user` string,\n" +
                "            url string,\n" +
                "            word string,\n" +
                "            length int\n" +
                "         ) with (\n" +
                "            'connector' = 'print'\n" +
                "         )");

        rst.executeInsert("output");
    }


    public static class MySplit extends TableFunction<Tuple2<String, Integer>>{
        public void eval(String str){
            String[] fileds = str.split("\\?");
            for (String field :
                    fileds) {
                collect(Tuple2.of(field, field.length()));
            }
        }
    }
}
