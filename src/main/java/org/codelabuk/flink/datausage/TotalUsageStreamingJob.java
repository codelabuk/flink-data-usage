package org.codelabuk.flink.datausage;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.codelabuk.flink.datausage.record.UsageRecord;
import org.codelabuk.flink.datausage.record.UsageRecordDeserializationSchema;

import java.time.Instant;

import static org.apache.flink.table.api.Expressions.$;

public class TotalUsageStreamingJob {

    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.get("topic", "input");
        String brokers = params.get("bootstrap.servers", "localhost:9092");

        final Configuration flinkConfig = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        KafkaSource<UsageRecord> source = KafkaSource.<UsageRecord>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new UsageRecordDeserializationSchema())
                .build();

        DataStream<UsageRecord> records =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        Table results = tEnv.fromDataStream(records)
                .groupBy($("account"))
                .select($("account"),
                        $("byteUsed").sum().as("totalUsage"),
                        $("ts").max().as("asOf"));

        results.execute().print();
    }
}
