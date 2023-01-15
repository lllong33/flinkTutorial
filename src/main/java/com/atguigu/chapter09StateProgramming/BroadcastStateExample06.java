package com.atguigu.chapter09StateProgramming;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;



// todo 待理解应用场景
public class BroadcastStateExample06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取用户行为事件流
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "buy")
        );

        // 定义行为模式流，代表了要检测的标准; 通过广播join实现
        DataStreamSource<Pattern> patternStream = env
                .fromElements(
                        new Pattern("login", "pay"),
                        new Pattern("login", "buy")
                );
        actionStream.print("actionStream>>>");
        patternStream.print("patternStream>>>");

        // 广播流, MapStateDescriptor 作为流之间状态传递.
        MapStateDescriptor<Void, Pattern> patternsDescriptor = new MapStateDescriptor<>(
                "patterns", Types.VOID, Types.POJO(Pattern.class) // POJO类作用?
        );
        BroadcastStream<Pattern> broadcastStream = patternStream.broadcast(patternsDescriptor); // dataStreamSource -> BroadcastStream

        actionStream.keyBy(f -> f.userId)
                .connect(broadcastStream)
                .process(new PatternEvaluator())
            .print(">>>");

        env.execute();

    }

    public static class PatternEvaluator extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
        // 保存上次用户行为
        ValueState<String> prevActionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("BroadcastConnectedStream.process() open()");
            prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastAction", Types.STRING));
        }

        @Override
        public void processElement(Action action, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            System.out.println("BroadcastConnectedStream.process() processElement()");
            Pattern pattern = ctx.getBroadcastState(
                    new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class))).get(null);

            String prevAction = prevActionState.value();
            if (pattern != null && prevAction != null) {
                // 如果前后两次行为都符合模式定义，输出一组匹配
                System.out.println(pattern.action1);
                System.out.println(pattern.action2);
                System.out.println(prevAction);
                System.out.println(action.action);
                if (pattern.action1.equals(prevAction) && pattern.action2.equals(action.action)) {
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }
            // 更新状态
            prevActionState.update(action.action);

        }

        @Override
        public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            System.out.println("BroadcastConnectedStream.process() processBroadcastElement()");
            // 拿到广播流数据, 写入到状态.
            BroadcastState<Void, Pattern> bcState = ctx.getBroadcastState(
                    new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class)));
            bcState.put(null, value);
        }
    }

     // 定义用户行为事件POJO类
    public static class Action {
        public String userId;
        public String action;

        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId=" + userId +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    // 定义行为模式POJO类，包含先后发生的两个行为
    public static class Pattern {
        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }


}
