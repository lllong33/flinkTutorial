package com.atguigu.chapter07ProcessFunction;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import com.atguigu.chapter06TimeAndWindow.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/*
=====================

UrlViewCountStream >>>>>>>>:11> UrlViewCount{url='./prod?id=2', count=1, windowStart=2022-06-20 00:10:20.0, windowEnd=2022-06-20 00:10:30.0}
UrlViewCountStream >>>>>>>>:10> UrlViewCount{url='./home', count=3, windowStart=2022-06-20 00:10:20.0, windowEnd=2022-06-20 00:10:30.0}
UrlViewCountStream >>>>>>>>:9> UrlViewCount{url='./prod?id=1', count=1, windowStart=2022-06-20 00:10:20.0, windowEnd=2022-06-20 00:10:30.0}
UrlViewCountStream >>>>>>>>:6> UrlViewCount{url='./cart', count=4, windowStart=2022-06-20 00:10:20.0, windowEnd=2022-06-20 00:10:30.0}
UrlViewCountStream >>>>>>>>:5> UrlViewCount{url='./fav', count=1, windowStart=2022-06-20 00:10:20.0, windowEnd=2022-06-20 00:10:30.0}
TopN2 >>>>>>>>>:1> ===============
window close time: 2022-06-20 00:10:30.0
No.1 url: ./cart; page view: 4
No.2 url: ./home; page view: 3
=====================

TODO: 还有挺多未理解，
    1.为什么window对应的全窗口函数 ProcessWindowFunction 不能直接解决？要考虑未到数据？
        这里应该是想使用 timer，采用这种解决方式。
        1. 引出 windowAll + process 与 window + process 区别？
            由于这里只能按照时间处理，所以没有差别。
    2. 触发器真的解决了迟到的数据？ 水位线超过窗口结束时间，也可能存在未到数据。
* */

public class KeyedProcessTopNV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        SingleOutputStreamOperator<String> urlCountStream = stream.keyBy(data -> true) // 这里只能是单一key，因为要按照时间来处理
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new UrlViewCountResult());
        urlCountStream.print("TopN2 >>>>>>>>");

        env.execute();
    }

    private static class UrlViewCountResult extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean bkey, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            HashMap<String, Long> urlCountMap = new HashMap<>();
            for (Event element :
                    elements) {
                String url = element.url;
                if (urlCountMap.containsKey(url)) {
                    urlCountMap.put(url, urlCountMap.get(url) + 1L);
                } else {
                    urlCountMap.put(url, 1L);
                }
            }
            System.out.println(urlCountMap.toString());

            ArrayList<Tuple2<String, Long>> mapList = new ArrayList<>();
            // 排序
            for (String key :
                    urlCountMap.keySet()) {
                mapList.add(Tuple2.of(key, urlCountMap.get(key)));
            }
            mapList.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2.f1.intValue() - o1.f1.intValue();
                }
            });

            // 格式化结果
            StringBuilder result = new StringBuilder();
            result.append("===========================\n");
//            System.out.println(mapList.size());
            for (int i = 0; i < 2; i++) {
                Tuple2<String, Long> tmp = mapList.get(i);
                String info = "page view No." + (i + 1) +
                        " url: " + tmp.f0 +
                        " page view: " + tmp.f1 +
                        " window start time: " + new Timestamp(context.window().getStart()) +
                        " window close time: " + new Timestamp(context.window().getEnd()) + "\n";
                result.append(info);
            }
            result.append("===========================\n");
            out.collect(result.toString());
        }
    }
}
