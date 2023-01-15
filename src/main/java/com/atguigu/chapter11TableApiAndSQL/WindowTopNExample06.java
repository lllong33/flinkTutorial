package com.atguigu.chapter11TableApiAndSQL;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class WindowTopNExample06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并分配时间戳、生成水位线
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L), //00:00:01
                        new Event("Bob", "./cart", 1000L),   //00:00:01
                        new Event("Alice", "./prod?id=1",  25 * 60 * 1000L), //00:25:00
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),  //00:55:00
                        new Event("Bob", "./prod?id=5", 60 * 60 * 1000L + 60 * 1000L), //01:00:00 + 00:01:00
                        new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),   //01:00:00 + 00:30:00
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L) //01:00:00 + 00:59:00
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );
        
        // 创建表环境、流转表并指定watermark，注册表，定义子查询得到用户的访问次数，定义TopN逻辑
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table eventTable = tableEnv.fromDataStream(eventStream, $("user"), $("url"), $("timestamp").rowtime().as("ts"));
        tableEnv.createTemporaryView("eventTable", eventTable);

        String subQuery = "select window_start, window_end, user, count(*) as cnt " +
                "from table(" +
                "   tumble(table eventTable, descriptor(ts), interval '1' hour)" +
                ") group by window_start, window_end, user";

        /*
        select
            *
        from (
            select *,
                row_number() over(partition by window_start, window_end order by cnt desc) as row_num
            from ( $subQuery )
        )
        where row_num <= 2
        * */
        String topNQuery =
                "select * from (" +
                "   select *, row_number() over(partition by window_start, window_end order by cnt desc) as row_num" +
                "   from (" + subQuery + ")" +
                ") where row_num <= 2";
//        System.out.println(topNQuery);
//        System.out.println(topNQuery.substring(170, 190));
        Table rst = tableEnv.sqlQuery(topNQuery);
        tableEnv.toDataStream(rst).print("top 2>>");

        env.execute();

    }
}
