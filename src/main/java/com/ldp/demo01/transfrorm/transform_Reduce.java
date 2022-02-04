package com.ldp.demo01.transfrorm;

import com.ldp.demo01.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.metrics.stats.Max;

public class transform_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.3.91", 9999);

        dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        }).keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }

        }).reduce(new ReduceFunction<WaterSensor>() {
            @Override
            //将取后一条数据的TS, 2个vc中最大的值
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                return new WaterSensor(value1.getId(), value2.getTs(), Double.max(value1.getVc(), value2.getVc()));
            }
        }).print();

//        result.print();

        env.execute();

    }
}
