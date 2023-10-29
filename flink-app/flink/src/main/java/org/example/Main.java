package org.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.mapping.Mapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

public class Main {
    public static void main(String[] args) throws Exception {

        final DeserializationSchema<BikeRide> schema = new DeserializationKafka();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<BikeRide> source = KafkaSource.<BikeRide>builder()
                .setBootstrapServers("kafka-server:29092")
                .setTopics("flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(schema))
                .build();

        DataStream<BikeRide> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").
                filter((FilterFunction<BikeRide>) value -> (value.rideable_type.equals("electric_bike")));
        DataStream<PopularStationStatistics> res = ds.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .process(new StatisticsStream());
        res.print();
        CassandraSink.addSink(res)
                .setMapperOptions(() -> new Mapper.Option[] {
                        Mapper.Option.saveNullFields(true)
                })
                .setClusterBuilder(new ClusterBuilder() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {

                        return builder.addContactPoints("cassandra-node").withPort(9042).build();
                    }
                })
                .build();
        env.setParallelism(2);
        env.execute("Big Data 2 - Flink");
    }
}