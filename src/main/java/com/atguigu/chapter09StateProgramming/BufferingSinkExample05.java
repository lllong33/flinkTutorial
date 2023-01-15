package com.atguigu.chapter09StateProgramming;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

// todo checkpoint 流程待测试
// 列表状态的平均分割重组
public class BufferingSinkExample05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // todo 并行情况下测试结果不同，待排查

        env.enableCheckpointing(10*1000L);

        CheckpointConfig ckptConfig = env.getCheckpointConfig();
        ckptConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // Sets the checkpointing mode (exactly-once vs. at-least-once).
        ckptConfig.setMinPauseBetweenCheckpoints(500); // 触发同时触发另外一个checkpoint 的间隔时间
        ckptConfig.setCheckpointTimeout(60*1000L); // 这里指执行checkpoint过程，timeout
        ckptConfig.setMaxConcurrentCheckpoints(1); // 并发，qst这里指单个slot嘛？
        //  enable externalized checkpoints which are retained after job cancellation
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        ckptConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        // 不再执行检查点的分界线对齐操作，启用之后可以大大减少产生背压时的检查点保存时间。这个设置要求检查点模式（CheckpointingMode）必须为exactly-once，并且并发的检查点个数为1
        ckptConfig.enableUnalignedCheckpoints();

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        stream.print("rawData>>>");

        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    private static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        private final int threshold;
        // 对于transient 修饰的成员变量，不会贯穿对象的序列化和反序列化，生命周期仅存于调用者的内存中而不会写到磁盘里进行持久化。
        private transient ListState<Event> checkpointedState;
        private List<Event> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            // 1、不断将元素写入缓存；2、达到阈值sink
            bufferedElements.add(value);
//            System.out.println(bufferedElements.size()); // 这里并行度必须设置1，否则出现状态访问问题。
            if (bufferedElements.size() >= threshold){
                for (Event element : bufferedElements) {
                    System.out.println(element);
                }
                System.out.println("=======输出完毕==========");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 快照
            checkpointedState.clear();
            for (Event element: bufferedElements){
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 恢复
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>(
                    "buffered-element",
                    Types.POJO(Event.class)
            );
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()){
                for (Event elemnt :
                        checkpointedState.get()) {
                    bufferedElements.add(elemnt);
                }
            }
        }
    }
}
