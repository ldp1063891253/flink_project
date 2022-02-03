package com.ldp.demo01.transfrorm;

import com.ldp.demo01.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.protocol.types.Field;

//RichFunction富有的地方在于 1.声明周期方法 2，可以执行上下文执行环境，做状态编程
public class transform_RichMapFun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(2);

        DataStreamSource<String> dataStreamSource = env.readTextFile("E:\\01.software\\06.Bigdata_demo\\flink-demo\\flink_project\\src\\main\\resources\\sensor.txt");

        SingleOutputStreamOperator<WaterSensor> streamOperator = dataStreamSource.map(new MyRichMapFun());

        streamOperator.print();

        env.execute();

    }

    public static class MyRichMapFun extends RichMapFunction<String, WaterSensor> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open 方法被使用");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            String[] split = value.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        }

        @Override
        public void close() throws Exception {
            System.out.println("close 方法被使用");
        }
    }
}
