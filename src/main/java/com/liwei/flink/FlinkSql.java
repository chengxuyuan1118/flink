package com.liwei.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.FlatMapIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import java.util.*;

public class FlinkSql {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "flink";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv;
        if (Objects.equals(planner, "blink")) {
            EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
            tableEnv = StreamTableEnvironment.create(env, settings);
        } else {
            if (!Objects.equals(planner, "flink")) {
                System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', where planner (it is either flink or blink, and the default is flink) indicates whether the example uses flink planner or blink planner.");
                return;
            }
            tableEnv = StreamTableEnvironment.create(env);
        }
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "group1");
        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>("k1", new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(myConsumer);
        DataStream<Log> dataStream = stream.flatMap(new FlatMapIterator<String, Log>() {
            @Override
            public Iterator<Log> flatMap(String value) throws Exception {
                List list = new ArrayList();
                Log log = null;
                String line[] = value.split("\r\n");
                for (String data : line) {
                    String v[] = data.split(",");
                    log = new Log();
                    log.setId(v[0]);
                    log.setpNo(v[1]);
                    log.setUp(v[2]);
                    log.setDown(v[3]);
                    list.add(log);
                }
                return list.iterator();
            }
        });
        //注册为表
        Table tableA = tableEnv.fromDataStream(dataStream, "id,pNo,up,down");
        Table sqlQuery = tableEnv.sqlQuery("select * from " + tableA);
        Table sqlQuery1 = tableEnv.sqlQuery("select up,down from " + tableA);
        //tableEnv.registerDataStream("log", dataStream, "id,pNo,up,down");
        sqlQuery.printSchema();
        sqlQuery1.printSchema();
        //Table 转化为 DataStream
        DataStream<Log> appendStream = tableEnv.toAppendStream(sqlQuery, Log.class);
        appendStream.print();
        DataStream<Tuple2<String, String>> tuple2DataStream = tableEnv.toAppendStream(sqlQuery1, Types.TUPLE(Types.STRING, Types.STRING));
        tuple2DataStream.print();
        env.execute("flink");
    }
}
