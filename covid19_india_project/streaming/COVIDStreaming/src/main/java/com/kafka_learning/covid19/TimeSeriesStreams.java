package com.kafka_learning.covid19;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class TimeSeriesStreams {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "time-series-covid19-app3");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        final Serializer<JsonNode> jsonNodeSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonNodeDeSerializer = new JsonDeserializer();
        final Serde<JsonNode> jsonNodeSerde = Serdes.serdeFrom(jsonNodeSerializer, jsonNodeDeSerializer);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, JsonNode> individualDetailsStream = streamsBuilder.stream("individual-details", Consumed.with(Serdes.String(), jsonNodeSerde));

        ObjectNode jsonOutputObject = JsonNodeFactory.instance.objectNode();
        jsonOutputObject.put("total", 0);
        jsonOutputObject.put("active", 0);
        jsonOutputObject.put("recovered", 0);
        jsonOutputObject.put("deceased", 0);

        KTable<String, Long> outputJsonTable = individualDetailsStream
                .selectKey((key, value) -> value.get("current_status").toString())
                .groupByKey(Grouped.with(Serdes.String(), jsonNodeSerde))
                .count();

        KStream<String, Long> outputJsonStream = outputJsonTable.toStream();

        outputJsonStream.peek((key, value) -> {
            System.out.println("Key : " + key.toLowerCase());
            System.out.println("Value : " + value.toString());
        });

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
