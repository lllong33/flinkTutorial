package com.atguigu.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        // create env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // source
        // 1. from file
        DataStreamSource<String> stream1 = env.readTextFile("./input/click.txt");

        // 2. from collection
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> stream2 = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream3 = env.fromCollection(events);

        // 3. from element
        DataStreamSource<Event> stream4 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        
        // 4. from socket
        DataStreamSource<String> stream5 = env.socketTextStream("node1", 7777);

        // 5. from kafka, use kafka-connect定义的SourceFunction
        /* 启动一个测试kafka生产者
        * ./bin/zookeeper-server-start.sh -daemon ./config/zookeeper.properties
        * ./bin/kafka-server-start.sh -daemon ./config/server.properties
        * ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic clicks
        * */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node1:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> stream6 = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        // transformation

        // sink
//        stream1.print("1");
//        stream2.print("2");
//        stream3.print("3");
//        stream4.print("4");
//        stream5.print("5");
        stream6.print("6");

        // 触发程序执行, 上面只是定义作业操作，然后添加到数据流图中。显示调用execute，并返回一个执行结果
        env.execute();


    }
}



