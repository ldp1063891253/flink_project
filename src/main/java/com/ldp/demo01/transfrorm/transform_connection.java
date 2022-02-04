package com.ldp.demo01.transfrorm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class transform_connection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //设置2个流
        DataStreamSource<String> string1 = env.socketTextStream("192.168.3.91", 8888);
        DataStreamSource<String> int2 = env.socketTextStream("192.168.3.91", 9999);

        SingleOutputStreamOperator<Integer> streamOperator = int2.map((MapFunction<String, Integer>) value -> value.length());

        // 流连接
        ConnectedStreams<String, Integer> connectedStreams =
                string1.connect(streamOperator);

        SingleOutputStreamOperator<Object> result = connectedStreams.map(new CoMapFunction<String, Integer, Object>() {
            @Override
            public Object map1(String value) throws Exception {
                return value;
            }

            @Override
            public Object map2(Integer value) throws Exception {
                return value;
            }

        });

        result.print();

        env.execute();

    }
}
