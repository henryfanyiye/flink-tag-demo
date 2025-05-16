package com.demo.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.core.datastream.source.JdbcSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/**
 * Flink JDBC 批处理示例，读取 MySQL 表数据
 *
 * @author henry.fan
 */
public class BatchQueryDemo {

    public static class Event {
        public int id;
        public String user_id;
        public String event_type;
        public String event_time;

        public Event(int id, String user_id, String event_type, String event_time) {
            this.id = id;
            this.user_id = user_id;
            this.event_type = event_type;
            this.event_time = event_time;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "id=" + id +
                    ", user_id='" + user_id + '\'' +
                    ", event_type='" + event_type + '\'' +
                    ", event_time='" + event_time + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        TableQueryDemo();
    }

    public static void DataStreamQueryDemo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 构造 JDBC Source
        JdbcSource<Event> jdbcSource = JdbcSource.<Event>builder()
                .setDriverName("com.mysql.cj.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/demo?useSSL=false&serverTimezone=UTC")
                .setUsername("root")
                .setPassword("12345678")
                .setSql("SELECT id, user_id, event_type, event_time FROM user_events")
                .setResultSetType(java.sql.ResultSet.TYPE_FORWARD_ONLY)
                .setSplitReaderFetchBatchSize(1)
                .setResultExtractor(rs ->
                        new Event(
                                rs.getInt("id"),
                                rs.getString("user_id"),
                                rs.getString("event_type"),
                                rs.getString("event_time")
                        )
                )
                .setTypeInformation(TypeInformation.of(Event.class))
                .build();

        // Source → Sink 打印
        env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .name("Read-MySQL-UserEvents")
                .addSink(new PrintSinkFunction<>())
                .name("Print-To-Console");

        env.execute("Flink JDBC Batch Read Demo");
    }

    public static void TableQueryDemo() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql(
                "CREATE TABLE user_events (\n" +
                        "  id INT,\n" +
                        "  user_id STRING,\n" +
                        "  event_type STRING,\n" +
                        "  event_time TIMESTAMP(3)\n" +
                        ") WITH (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                        "  'url' = 'jdbc:mysql://localhost:3306/demo',\n" +
                        "  'table-name' = 'user_events',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = '12345678',\n" +
                        "  'scan.fetch-size' = '10000'\n" +
                        ")"
        );

        // 查询 & 更新
        tableEnv.executeSql(
                "SELECT * FROM user_events where id = 6"
        ).print();
    }
}
