package com.atguigu.chapter12FlinkCEP;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class OrderTimeoutDetect02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取订单事件流，并提取时间戳、生成水位线
        KeyedStream<OrderEvent, String> stream = env
                .fromElements(
                        new OrderEvent("user_1", "order_1", "create", 1000L),
                        new OrderEvent("user_2", "order_2", "create", 2000L),
                        new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                        new OrderEvent("user_1", "order_1", "pay", 60 * 1000L), // 00:01:00
                        new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)// 00:20:00
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<OrderEvent>() {
                                            @Override
                                            public long extractTimestamp(OrderEvent event, long l) {
                                                return event.timestamp;
                                            }
                                        }
                                )
                )
                .keyBy(order -> order.orderId);    // 按照订单ID分组

        // 1.define pattern
        // 2.apply pattern, check match complex event, get PatternStream
        // 3.extract the complex events that match the timeout part, and the warp into a prompt output
        // 4.Defines a side output stream label that identifies a timeout side output stream
        // 5.Prints a stream of processing results for normal and timeout partial matches
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                .within(Time.minutes(15));

        SingleOutputStreamOperator<String> payedOrderStream = CEP.pattern(stream, pattern)
                .process(new OrderPayPatternProcessFunction());

        OutputTag<String> stringOutputTag = new OutputTag<String>("timeout"){};

        payedOrderStream.print("payed>>");
        payedOrderStream.getSideOutput(stringOutputTag).print("timeout>>>");

        env.execute();
    }

    private static class OrderPayPatternProcessFunction extends PatternProcessFunction<OrderEvent, String> implements TimedOutPartialMatchHandler<OrderEvent> {
        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) throws Exception {
            OrderEvent pay = match.get("pay").get(0);
            out.collect("order: " + pay.orderId + " payed! ");
        }

        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context ctx) throws Exception {
            OrderEvent create = match.get("create").get(0);
            OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){}; // qst 这种后面带{}，是什么含义？
            ctx.output(timeoutTag, "order: " + create.orderId + " Overdue payment!" + create.userId);
        }
    }
}
