package com.ldp.demo01.transfrorm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class transform_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //设置2个流
        DataStreamSource<String> dataStreamSource1 = env.socketTextStream("192.168.3.91", 8888);
        DataStreamSource<String> dataStreamSource2 = env.socketTextStream("192.168.3.91", 9999);

        DataStream<String> result = dataStreamSource1.union(dataStreamSource2);



        result.print();

        env.execute();

    }
}
