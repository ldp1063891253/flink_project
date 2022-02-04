package com.ldp.demo01.transfrorm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

public class transform_process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //设置2个流
        DataStreamSource<String> string1 = env.socketTextStream("192.168.3.91", 8888);

        SingleOutputStreamOperator<String> singleOutputStreamOperator = string1.process(new MyProcessMapFun());

        SingleOutputStreamOperator<Tuple2<String, Integer>> process = singleOutputStreamOperator.process(new MyProcessMapFun02());

        process.keyBy(data -> data.f0).sum(1).print();


        env.execute();

    }


    private static class MyProcessMapFun extends ProcessFunction<String,String> {
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            String[] strings = value.split(",");
            for (String string : strings) {
                out.collect(string);
            }
        }
    }

    private static class MyProcessMapFun02 extends ProcessFunction<String, Tuple2<String,Integer>> {
        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value, 1));
        }
    }
}
