package com.atguigu.chapter06TimeAndWindow;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/*
事件时间来到12000，即水位线为1000，触发窗口计算；统计result；
由于设置了窗口late的1分钟，事件时间来到10s+60s+2s=72s，关闭窗口，后续数据写入late测输出流；

一个事件时间，可以推导出：水位线，该条数据窗口，触发窗口计算水位线，触发窗口关闭的水位线（窗口late时间）

qst 为什么窗口范围，不根据水位线来判断，使用事件时间？

pvWithWMDS>>>>>>>>>>>> Event{user='Alice', url='./home', timestamp=1970-01-01 08:00:01.0} //这条数据属于[0.10)窗口，需要水位线达到10+2s触发触发窗口计算, 会接收[0,12+60)期间数据；
pvWithWMDS>>>>>>>>>>>> Event{user='Alice', url='./home', timestamp=1970-01-01 08:00:02.0} // 属于[0,10)窗口
pvWithWMDS>>>>>>>>>>>> Event{user='Alice', url='./home', timestamp=1970-01-01 08:00:10.0}  //这条数据触发窗口计算,属于[10.20)窗口，需要水位线达到22s触发, 会接收[0,22+60)期间数据；
pvWithWMDS>>>>>>>>>>>> Event{user='Alice', url='./home', timestamp=1970-01-01 08:00:09.0}
pvWithWMDS>>>>>>>>>>>> Event{user='Alice', url='./cart', timestamp=1970-01-01 08:00:12.0}
result>>>>>>>> UrlViewCount{url='./home', count=3, windowStart=1970-01-01 08:00:00.0, windowEnd=1970-01-01 08:00:10.0}
pvWithWMDS>>>>>>>>>>>> Event{user='Alice', url='./prod?id=100', timestamp=1970-01-01 08:00:15.0}  //这条数据触发窗口计算,属于[10.20)窗口，需要水位线达到22s触发, 会接收[0,22+60)期间数据；
pvWithWMDS>>>>>>>>>>>> Event{user='Alice', url='./home', timestamp=1970-01-01 08:00:09.0}
result>>>>>>>> UrlViewCount{url='./home', count=4, windowStart=1970-01-01 08:00:00.0, windowEnd=1970-01-01 08:00:10.0}
pvWithWMDS>>>>>>>>>>>> Event{user='Alice', url='./home', timestamp=1970-01-01 08:00:08.0}
result>>>>>>>> UrlViewCount{url='./home', count=5, windowStart=1970-01-01 08:00:00.0, windowEnd=1970-01-01 08:00:10.0}
pvWithWMDS>>>>>>>>>>>> Event{user='Alice', url='./prod?id=200', timestamp=1970-01-01 08:01:10.0}
result>>>>>>>> UrlViewCount{url='./cart', count=1, windowStart=1970-01-01 08:00:10.0, windowEnd=1970-01-01 08:00:20.0}
result>>>>>>>> UrlViewCount{url='./home', count=1, windowStart=1970-01-01 08:00:10.0, windowEnd=1970-01-01 08:00:20.0}
result>>>>>>>> UrlViewCount{url='./prod?id=100', count=1, windowStart=1970-01-01 08:00:10.0, windowEnd=1970-01-01 08:00:20.0}
pvWithWMDS>>>>>>>>>>>> Event{user='Alice', url='./home', timestamp=1970-01-01 08:00:08.0}
result>>>>>>>> UrlViewCount{url='./home', count=6, windowStart=1970-01-01 08:00:00.0, windowEnd=1970-01-01 08:00:10.0}
pvWithWMDS>>>>>>>>>>>> Event{user='Alice', url='./prod?id=300', timestamp=1970-01-01 08:01:12.0} // 达到72s，关闭[0,10)窗口，后续该窗口数据写入侧输出流 late；注意这里水位线处理窗口计算，窗口关闭
pvWithWMDS>>>>>>>>>>>> Event{user='Alice', url='./home', timestamp=1970-01-01 08:00:08.0}
late>>>>>>> Event{user='Alice', url='./home', timestamp=1970-01-01 08:00:08.0}
result>>>>>>>> UrlViewCount{url='./prod?id=300', count=1, windowStart=1970-01-01 08:01:10.0, windowEnd=1970-01-01 08:01:20.0}
result>>>>>>>> UrlViewCount{url='./prod?id=200', count=1, windowStart=1970-01-01 08:01:10.0, windowEnd=1970-01-01 08:01:20.0}
* */
public class ProcessLateDateExample {
    public static void main(String[] args) throws Exception {
        // 1.get env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.get stream, map Event, assign WM
        DataStreamSource<String> textDS = env.readTextFile("./input/PLDE.txt");
        SingleOutputStreamOperator<Event> pvWithWMDS = textDS.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                String[] split = value.split(",");
                Thread.sleep(1000*3);
                return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 3.define OutputTag， set window(need pre keyBy), sideOutputLateData，
        OutputTag<Event> outputTag = new OutputTag<Event>("late") {}; // TODO 关于OutputTag概念

        SingleOutputStreamOperator<UrlViewCount> result = pvWithWMDS.keyBy(value -> value.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // TODO 关于窗口概念
                // 方法二：允许窗口处理迟到数据，设置1分组等待时间
                .allowedLateness(Time.minutes(1))
                // 方法三：将最后迟到数据输出到测输出流
                .sideOutputLateData(outputTag)
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());// todo 未理解

        result.print("result>>>>>>>");
        result.getSideOutput(outputTag).print("late>>>>>>"); // qstA 为啥这里不能计算出 UrlViewCount？双流join的方式合并，在聚合即可
        pvWithWMDS.print("pvWithWMDS>>>>>>>>>>>");

        // 4.execute
        env.execute("UrlViewCount");
    }

    // 工作原理：首先调用createAccumulator()为任务初始化一个状态（累计器）；而后每来一个数据调用一次add()方法，对数据进行聚合，得到结果保存在状态中；等到了窗口需要输出时，再调用getResult()方法得到计算结果。
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>{

        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }

}
