package com.ldp.flink.window;




import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Tumbling Windows
 */
public class TimeTumbling {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("node01", 9999);


        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {


            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(" ");
                for (int i = 0; i < split.length; i++) {
                    out.collect(new Tuple2<>(split[i], 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamOperator.keyBy(data -> data.f0);


        //开窗
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //增量聚合计算
        //第一种方式
//       SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.sum(1);

        //第二种方式
//        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.aggregate(new MyAggFun(), new MyWindowFun());

        //第三种方式
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                //迭代器的长度  window.getStart())窗口时间
                ArrayList<Tuple2<String, Integer>> tuple2ArrayList = Lists.newArrayList(elements.iterator());
                out.collect(new Tuple2<>(new Timestamp(context.window().getStart()) + ":" + key, tuple2ArrayList.size()));
            }
        });


        //全量窗口 apply
/*        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {

                  //迭代器的长度  window.getStart())窗口时间
                ArrayList<Tuple2<String, Integer>> tuple2ArrayList = Lists.newArrayList(input.iterator());
                out.collect(new Tuple2<>(new Timestamp(window.getStart())+":"+tuple.toString(), tuple2ArrayList.size()));

            }
        });*/
        result.print();



        env.execute();

    }

    private static class MyAggFun implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return 1+accumulator;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    private static class MyWindowFun implements WindowFunction<Integer,Tuple2<String, Integer>,String,TimeWindow>{

        @Override
        public void apply(String key, TimeWindow window, Iterable<Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                    //取出迭代器中的数据
            Integer integer = input.iterator().next();
            out.collect(new Tuple2<>(new Timestamp(window.getStart())+":"+key, integer));
        }
    }
}
