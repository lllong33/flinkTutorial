package com.atguigu.chapter05;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import com.starrocks.connector.flink.table.sink.StarRocksDynamicSinkFunctionV2;
import org.apache.flink.table.factories.DynamicTableFactory;


/**
 * Created by Enzo Cotter on 2022/11/26.
 */
public class SinkStarRocks {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // -------- 原始数据为 CSV 格式 --------
        // create a table with `structure` and `properties`
        // Needed: Add `com.starrocks.connector.flink.table.StarRocksDynamicTableSinkFactory`
        //         to: `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`
        TableResult tableResult = tEnv.executeSql(
                "CREATE TABLE USER_RESULT(" +
                        "name VARCHAR," +
                        "score BIGINT" +
                        ") WITH ( " +
                        "'connector' = 'starrocks'," +
                        "'jdbc-url'='jdbc:mysql://192.168.39.104:9030,192.168.39.105:9030,192.168.39.106:9030'," +
                        "'load-url'='192.168.39.104:8030;192.168.39.105:8030;192.168.39.106:8030'," +
                        "'database-name' = 'ads_ka'," +
                        "'table-name' = 'user_rst_test'," +
                        "'username' = 'ads_ka_rd'," +
                        "'password' = 'ads_ka_rd@13b705'," +
                        "'sink.buffer-flush.max-rows' = '1000000'," +
                        "'sink.buffer-flush.max-bytes' = '300000000'," +
                        "'sink.buffer-flush.interval-ms' = '5000'," +
                        // 自 2.4 版本，支持更新主键模型中的部分列。您可以通过以下两个属性指定需要更新的列。
                        // "'sink.properties.partial_update' = 'true'," +
                        // "'sink.properties.columns' = 'k1,k2,k3'," +
                        "'sink.properties.column_separator' = '\\x01'," +
                        "'sink.properties.row_delimiter' = '\\x02'," +
//        "'sink.properties.*' = 'xxx'" + // stream load properties like `'sink.properties.columns' = 'k1, v1'`
                        "'sink.max-retries' = '3'" +
                        ")"
        );

//        Table table = tEnv.sqlQuery("select name, score from USER_RESULT");

//        tEnv.toDataStream(table).print(">>>>");
    }

}
