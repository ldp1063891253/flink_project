package com.ldp.demo01.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Source_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.91:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "12");
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("gptest", new SimpleStringSchema(), properties));
        dataStreamSource.print();
        env.execute();


    }
}
