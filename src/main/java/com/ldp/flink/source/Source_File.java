package com.ldp.flink.source;

import com.ldp.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("E:\\01.software\\06.Bigdata_demo\\flink-demo\\flink_project\\src\\main\\resources\\sensor.txt");

        dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        }).print();

        env.execute();
    }
}
