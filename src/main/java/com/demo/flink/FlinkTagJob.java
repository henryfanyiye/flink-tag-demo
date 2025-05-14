package com.demo.flink;

import com.demo.flink.model.UserTagEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * @author henry.fan
 */
public class FlinkTagJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 启用 Checkpoint（必须）
        env.enableCheckpointing(5000);
        // 开发测试用
        env.setParallelism(1);

        // 设置 Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("user_tag_topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "1000")
                .build();

        // 从 Kafka 消费数据流
        DataStream<String> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 解析 JSON 为 POJO
        DataStream<UserTagEvent> parsedStream = rawStream
                .flatMap((String value, Collector<UserTagEvent> out) -> {
                    try {
                        UserTagEvent event = new ObjectMapper().readValue(value, UserTagEvent.class);
                        out.collect(event);
                    } catch (Exception e) {
                        System.err.println(e);
                        System.err.println("Invalid message skipped: " + value);
                    }
                })
                .name("Safe JSON Parser")
                // 显式声明返回类型
                .returns(UserTagEvent.class);

        parsedStream.print();

        env.execute("Flink Tag Job with Kafka to Doris");
    }
}
