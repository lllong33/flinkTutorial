package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

         //source 注意 element parallelism 只能是 1
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Alice", "./prod?id=200", 3200L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
        );

        // 1. 随机分区
//        stream.shuffle().print().setParallelism(4);

        // 2. 轮询分区 底层默认使用
//        stream.rebalance().print().setParallelism(4);

        // 3. rescale 重缩放分区, 避免跨 taskManager 产生网络IO
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 0; i < 8; i++) {
                    // 将奇偶数分别发送到0和1号并行分区
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask())
                        ctx.collect(i);
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2)
//                .rescale() // 0 号分区对应 0 2 4 6（对应 slot 1 2 或者 3 4）, 1号分区对应 1 3 5 7
//                .rebalance() // 0 号分区对应 0 2 4 6（对应 slot 为 1 2 3 4）, 1号分区对应 1 3 5 7
//                .print()
                .setParallelism(4);

        // 4. 广播 每一条数据都会被 slot 处理一次
//        stream.broadcast().print().setParallelism(4);

        // 5. 全局分区 放到单一分区
//        stream.global().print().setParallelism(4);

        // 6. 自定义重分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .print()
                .setParallelism(4);

        env.execute();
    }
}



