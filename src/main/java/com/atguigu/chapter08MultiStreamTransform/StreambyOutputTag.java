package com.atguigu.chapter08MultiStreamTransform;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class StreambyOutputTag {
    // 对于 OutputTag 的 T，都可以给什么类型？为什么给 Event 会有问题
    // org.apache.flink.api.common.functions.InvalidTypesException: The types of the interface org.apache.flink.util.OutputTag could not be inferred. Support for synthetic interfaces, lambdas, and generic or raw types is limited at this point
    private static OutputTag<Tuple3<String, String, Long>> specialSideStream = new OutputTag<Tuple3<String, String, Long>>("specialSideStream"){}; // qst为啥一定要带{}？

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                // 可以将数据放到测输出流，窗口，process
                .process(new ProcessFunction<Event, Event>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                        if (value.user.equals("Alice")) {
                            ctx.output(specialSideStream, new Tuple3<String, String, Long>(value.user, value.url, value.timestamp));
                        }
                        out.collect(value);
                    }
                });

        // side output 特征
        stream.print("normal data>>>>>>");
        stream.getSideOutput(specialSideStream).print("specialSideStream>>>>>");

        env.execute();

    }
}
