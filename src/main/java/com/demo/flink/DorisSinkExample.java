package com.demo.flink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * @author henry.fan
 */
public class DorisSinkExample {

    public static void main(String[] args) throws Exception {
        JSONFormatWrite();
    }

    public static void JSONFormatWrite() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(30000);

        DorisSink.Builder<String> builder = DorisSink.builder();

        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("10.10.11.56:8030")
                .setTableIdentifier("tag_db.user_tags_kafka")
                .setUsername("admin")
                .setPassword("")
                .build();

        Properties properties = new Properties();
        properties.setProperty("read_json_by_line", "true");
        properties.setProperty("format", "json");

        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setLabelPrefix("label-doris" + UUID.randomUUID())
                        .setDeletable(false)
                        .setBatchMode(true)
                        .setStreamLoadProp(properties)
                        .build();

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisOptions);

        List<String> data = new ArrayList<>();
        data.add("{\"id\":3,\"name\":\"Michael\",\"age\":28}");
        data.add("{\"id\":4,\"name\":\"David\",\"age\":38}");

        env.fromCollection(data).sinkTo(builder.build());
        env.execute("doris test");
    }
}