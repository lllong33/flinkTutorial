package com.atguigu.chapter06TimeAndWindow;

import com.atguigu.chapter05.ClickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.atguigu.chapter05.Event;

// 自定义水位线的产生
public class CustomWaterMarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .print();

        env.execute();
    }


    private static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {
        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            // todo 如何确定使用 SerializableTimestampAssigner 的？TimestampAssigner没有说明产生的方式
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp;
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }

    }

    // todo 不是很理解这个流程
    public static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {
        private Long delayTime=5000L;
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察最大时间戳 这里加1是避免初始化时，后续-1出现负数。
        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(event.timestamp, maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 默认200ms系统调用一次
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }
}
