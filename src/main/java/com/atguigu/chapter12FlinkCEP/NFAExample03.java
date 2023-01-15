package com.atguigu.chapter12FlinkCEP;

import com.atguigu.chapter09StateProgramming.ValueStateTest;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.lucene.index.Term;

import java.io.Serializable;

// 用户连续登录失败三次的用例，实现NFA Nondeterministic Finite Automation
public class NFAExample03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取登录事件流，这里与时间无关，就不生成水位线了
        KeyedStream<LoginEvent, String> stream = env.fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
                )
                .keyBy(r -> r.userId);

        stream.flatMap(new StateMachineMapper())
                .print("waring>>>>");

        env.execute();
    }

    @SuppressWarnings("serial")
    private static class StateMachineMapper extends RichFlatMapFunction<LoginEvent, String> {
        // 用来保存当前状态机的位置
        private ValueState<State> currentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            currentState = getRuntimeContext().getState(new ValueStateDescriptor<State>("state", State.class));
        }

        @Override
        public void flatMap(LoginEvent event, Collector<String> out) throws Exception {
            // 获取状态
            State state = currentState.value();
            if (state == null){
                state = State.Initial;
            }
            // 基于当前状态，获取下一状态
            State nextState = state.transition(event.eventType);

            // 根据当前事件获取的状态，进行逻辑判断
            if (nextState == State.Matched){
                out.collect(event.userId + " three consecutive login fail");
            } else if (nextState == State.Terminal){
                currentState.update(State.Initial);
            } else {
                currentState.update(nextState);
            }

        }
    }

    public enum State{
        Terminal,
        Matched,
        S2(new Transition("fail", Matched), new Transition("success", Terminal)),
        S1(new Transition("fail", S2), new Transition("success", Terminal)),
        Initial(new Transition("fail", S1), new Transition("success", Terminal));

        private final Transition[] transitions;

        State(Transition... transitions) {
            this.transitions = transitions;
        }

        // 根据输入事件类型，找到下一个状态
        public State transition(String eventType){
            for (Transition t: transitions) {
                if (t.getEventType().equals(eventType)){
                    return t.getTargetState();
                }
            }

            return null;
        }
    }

    public static class Transition implements Serializable{ //为什么一定要实现序列化接口？
        private static final long seriaVersionUID = 1L;

        private final String eventType; // 触发状态转移的当前事件类型

        private final State targetState; // 转移的目标状态

        public Transition(String eventType, State targetState){
            this.eventType = eventType;
            this.targetState = targetState;
        }

        public String getEventType() {
            return eventType;
        }

        public State getTargetState() {
            return targetState;
        }
    }
}
