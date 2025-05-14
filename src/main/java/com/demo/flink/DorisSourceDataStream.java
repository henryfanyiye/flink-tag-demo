package com.demo.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.source.DorisSource;

import java.util.List;
import java.util.Properties;

/**
 * @author henry.fan
 */
public class DorisSourceDataStream {

    public static void main(String[] args) throws Exception {
        useArrowFlightSQLRead();
    }

    public static void useArrowFlightSQLRead() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DorisOptions option =
                DorisOptions.builder()
                        .setFenodes("10.10.11.56:8030")
                        .setTableIdentifier("tag_db.user_tags_kafka")
                        .setUsername("admin")
                        .setPassword("")
                        .build();

        DorisReadOptions readOptions =
                DorisReadOptions.builder()
                        .setUseFlightSql(true)
                        .setFlightSqlPort(29747)
                        .setFilterQuery("age > 1")
                        .build();

        DorisSource<List<?>> dorisSource =
                DorisSource.<List<?>>builder()
                        .setDorisOptions(option)
                        .setDorisReadOptions(readOptions)
                        .setDeserializer(new SimpleListDeserializationSchema())
                        .build();

        env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris source").print();
        env.execute("Doris Source Test");
    }

    public static void useThriftRead() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DorisOptions option =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("test.students")
                        .setUsername("root")
                        .setPassword("")
                        .build();

        DorisReadOptions readOptions = DorisReadOptions.builder().build();
        DorisSource<List<?>> dorisSource =
                DorisSource.<List<?>>builder()
                        .setDorisOptions(option)
                        .setDorisReadOptions(readOptions)
                        .setDeserializer(new SimpleListDeserializationSchema())
                        .build();

        env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris source").print();
        env.execute("Doris Source Test");
    }

    public static void useSourceFunctionRead() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put("fenodes", "127.0.0.1:8030");
        properties.put("username", "root");
        properties.put("password", "");
        properties.put("table.identifier", "test.students");
        DorisStreamOptions options = new DorisStreamOptions(properties);

        env.setParallelism(2);
        env.addSource(new DorisSourceFunction(options, new SimpleListDeserializationSchema()))
                .print();
        env.execute("Flink doris test");
    }
}