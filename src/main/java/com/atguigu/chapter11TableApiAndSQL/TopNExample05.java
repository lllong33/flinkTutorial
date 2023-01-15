package com.atguigu.chapter11TableApiAndSQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TopNExample05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ddl define watermark
        String clickDDL = "create table clickTable(" +
                "`user` string," +
                "url string," +
                "ts bigint," +
                "et as to_timestamp(from_unixtime(ts/1000))," +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
               ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.csv', " +
                " 'format' =  'csv' " +
                ")";
        tableEnv.executeSql(clickDDL);
        
        // topN, 取用户中浏览量最大的2个
        Table topNrstTable = tableEnv.sqlQuery("SELECT user, cnt, row_num " +
                "FROM (" +
                "   SELECT *, ROW_NUMBER() OVER (" +
                "      ORDER BY cnt DESC" +
                "   ) AS row_num " +
                "   FROM (SELECT user, COUNT(url) AS cnt FROM clickTable GROUP BY user)" +
                ") WHERE row_num <= 2");
        tableEnv.toChangelogStream(topNrstTable).print("Top 2: ");

        env.execute();
    }
}
