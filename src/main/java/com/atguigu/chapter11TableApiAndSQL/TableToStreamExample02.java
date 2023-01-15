package com.atguigu.chapter11TableApiAndSQL;

import com.atguigu.chapter05.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableToStreamExample02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // stream -> virtual table, 直接在sql中使用该表
        tableEnv.createTemporaryView("EventTable", eventStream);

        // 查询Alice访问的url列表
        Table aliceVisitT = tableEnv.sqlQuery("select url, user from EventTable where user='Alice'");

        // 统计每个用户点击次数
        Table clickT = tableEnv.sqlQuery("select user, count(1) as cnt from EventTable group by user");

        // table -> stream print
        tableEnv.toDataStream(aliceVisitT).print("aliceVisitT>>>");
        tableEnv.toChangelogStream(clickT).print("clickT>>>>");

        env.execute();
    }
}
