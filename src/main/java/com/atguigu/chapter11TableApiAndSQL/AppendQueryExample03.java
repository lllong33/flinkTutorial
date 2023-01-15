package com.atguigu.chapter11TableApiAndSQL;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

// 聚合结果保持不变的案例：假如统计每小时用户点击次数，利用滚动窗口，将更新操作转为插入操作，即将一个小时数据聚合后再insert写入动态表。这里user+endT就是主键不会重复了。
public class AppendQueryExample03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> stream = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 创建表环境，流转表，注册表，滚动窗口计算每小时用户点击
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // Declares a field as the rowtime attribute for indicating, accessing, and working in Flink's  event time.
        Table eventTable = tableEnv.fromDataStream(stream, $("user"), $("url"), $("timestamp").rowtime().as("ts"));

        tableEnv.createTemporaryView("EventTable", eventTable);

        Table result = tableEnv.sqlQuery("select user, window_end as endT, count(url) as cnt " +
                                        "from table( " +
                                                "TUMBLE( table EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR)" +
                                                ")" +
                                        "group by user, window_start, window_end");

        tableEnv.toDataStream(result).print(">>>");

        env.execute();
    }
}
