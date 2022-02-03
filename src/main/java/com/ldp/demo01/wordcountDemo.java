package com.ldp.demo01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * flink  - wordcount
 */
public class wordcountDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.readTextFile("E:\\01.software\\06.Bigdata_demo\\flink-demo\\flink_project\\src\\main\\resources\\hello.txt");
        FlatMapOperator<String, String> flatMapOperator = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] strs = value.split(" ");
                for (int i = 0; i < strs.length; i++) {
                    out.collect(strs[i]);

                }
            }
        });

        MapOperator<String, Tuple2<String, Integer>> mapOperator = flatMapOperator.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        });

        AggregateOperator<Tuple2<String, Integer>> result = mapOperator.groupBy(0).sum(1);
        result.print();

//        env.execute();

    }
}
