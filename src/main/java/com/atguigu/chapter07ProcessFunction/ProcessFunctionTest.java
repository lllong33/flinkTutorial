package com.atguigu.chapter07ProcessFunction;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                            .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                @Override
                                public long extractTimestamp(Event element, long recordTimestamp) {
                                    return element.timestamp;
                                }
                            })
                )
                .process(new ProcessFunction<Event, Object>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<Object> out) throws Exception {
                        if (value.user.equals("Mary")){
                            out.collect(value);
                        } else if (value.user.equals("Bob")){
                            out.collect(value);
                            out.collect(value);
                        }
                        // timestamp => formatDT
                        DateTimeFormatter formater = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        LocalDateTime localDateTime = LocalDateTime.ofInstant(new Date(ctx.timerService().currentWatermark()).toInstant(), ZoneId.systemDefault());
                        System.out.println(formater.format(localDateTime));
                    }
                })
                .print("ProcessFunctionTest>>>>>");

        env.execute();
    }
}
