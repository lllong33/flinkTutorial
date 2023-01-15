package com.atguigu.chapter02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //
        ParameterTool pt = ParameterTool.fromArgs(args);
        String hostname = pt.get("host");
        Integer port = pt.getInt("port", 0);

        // 2. 从文件读取数据 按行读取
        DataStreamSource<String> lineDSS = hostname == null ? env.socketTextStream("node1", 7777) : env.socketTextStream(hostname, port);
        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS.flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                }
        ).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));//当Lambda表达式使用 Java 泛型的时候，由于泛型擦除的存在，需要显示的声明类型信息
        // 4. 按照 word 进行分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);
        // 5. 分组内聚合统计
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKS.sum(1);
        // 6. 打印结果
        sum.print();
        // 7.执行
        env.execute();
    }
}
