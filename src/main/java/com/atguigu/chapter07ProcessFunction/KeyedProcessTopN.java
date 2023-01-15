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
    2. 触发器真的解决了迟到的数据？ 水位线超过窗口结束时间，也可能存在未到数据。
* */

public class KeyedProcessTopN {
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

        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountAgg()
                        , new UrlViewCountResult());
        urlCountStream.print("UrlViewCountStream >>>>>>>>");
        SingleOutputStreamOperator<String> result = urlCountStream.keyBy(data -> data.windowEnd).process(new TopN(2));
        result.print("TopN2 >>>>>>>>>");

        env.execute();
    }

    private static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1L;
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

    private static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }

    // 自定义处理函数
    private static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {
        private Integer n;
        private ListState<UrlViewCount> urlViewCountListState; // 缓存之前已到达的数据，这里可能存在内存溢出的风险

        public TopN(Integer n){this.n = n;}

        @Override
        public void open(Configuration parameters) throws Exception {
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-view-count-list"
                        , Types.POJO(UrlViewCount.class))
            );
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 将count数据添加到列表状态中，保存起来
            urlViewCountListState.add(value);
            // 注册 window end + 1ms 后定时器，等待所有数据到齐排序求TopN
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCount urlViewCount :
                    urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }
            urlViewCountListState.clear();

            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            // format TopN2
            StringBuffer result = new StringBuffer();
            result.append("===============\n");
            result.append("window close time: " + new Timestamp(timestamp - 1)+ "\n");
            for (int i = 0; i < 2; i++) {
                UrlViewCount urlViewCount = urlViewCountArrayList.get(i);
                String info = "No." + (i+1) +
                        " url: " + urlViewCount.url +
                        "; page view: " + urlViewCount.count + "\n";
                result.append(info);
            }
            result.append("=====================\n");
            out.collect(result.toString());
        }
    }
}
