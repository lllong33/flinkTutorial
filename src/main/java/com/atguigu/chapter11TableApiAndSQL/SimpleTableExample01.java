package com.atguigu.chapter11TableApiAndSQL;

import com.atguigu.chapter05.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class SimpleTableExample01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L),
                new Event("Bob", "./prod?id=3", 90 * 1000L),
                new Event("Alice", "./prod?id=7", 105 * 1000L)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table eventTable = tableEnv.fromDataStream(stream);

        // 通过SQL提取数据
        Table resultTable1 = tableEnv.sqlQuery("select url, user from " + eventTable);

        // 通过TableAPI提取数据
        Table resultTable2 = eventTable.select($("user"), $("url"))
                .where($("user").isEqual("Alice"));

        // table -> stream
        tableEnv.toDataStream(resultTable1).print("table1>>>>");
        tableEnv.toDataStream(resultTable2).print("table2>>>>");

        env.execute();
    }
}
