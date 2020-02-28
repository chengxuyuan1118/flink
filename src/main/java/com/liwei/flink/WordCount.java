package com.liwei.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import java.util.List;

public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile("D:\\data.txt");
        //DataSet<String> text = env.readTextFile("/opt/data/data.txt");
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Count()).groupBy(0).sum(1);
        counts.print();
    }

    public static class Count implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {

            String[] datas = value.split(",");

            for (String data : datas) {
                if (data.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(data, 1));
                }
            }
        }
    }

}
