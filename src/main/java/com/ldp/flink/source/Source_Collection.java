package com.ldp.flink.source;

import com.ldp.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;


/**
 * 从集合中进行读取
 */
public class Source_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        executionEnvironment.setParallelism(1);

        List<WaterSensor> waterSensorList = Arrays.asList(new WaterSensor("21", 1212L, 12.1),
                new WaterSensor("21", 1212L, 12.1),
                new WaterSensor("21", 1212L, 12.1));

        DataStreamSource<WaterSensor> result= executionEnvironment.fromCollection(waterSensorList);

        result.print();

        executionEnvironment.execute("result");

    }
}
