package com.ldp.demo01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flink  - wordcount
 */
public class wordcountDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining();
        DataStream<String> dataSource = env.readTextFile("E:\\01.software\\06.Bigdata_demo\\flink-demo\\flink_project\\src\\main\\resources\\hello.txt");
        SingleOutputStreamOperator<String> streamOperator = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] strs = value.split(" ");
                for (int i = 0; i < strs.length; i++) {
                    out.collect(strs[i]);

                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> outputStreamOperator = streamOperator.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        }).slotSharingGroup("");
            outputStreamOperator.keyBy(0).sum(1).print();

        env.execute();

    }
}
