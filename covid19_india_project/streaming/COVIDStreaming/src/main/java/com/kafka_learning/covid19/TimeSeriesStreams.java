package com.kafka_learning.covid19;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

public class TimeSeriesStreams {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "time-series-covid19-app15");
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

        ObjectNode jsonOutputObject = new ObjectMapper().createObjectNode();
//        jsonOutputObject.put("total", 0);
//        jsonOutputObject.put("active", 0);
        jsonOutputObject.put("hospitalized", 0);
        jsonOutputObject.put("recovered", 0);
        jsonOutputObject.put("deceased", 0);

        KStream<String, JsonNode> outputJsonTable = individualDetailsStream
                .selectKey((key, value) -> value.get("current_status").toString().toLowerCase().replace("\"", ""))
                .filterNot((filterKey, filterValue) -> filterKey.equals("migrated") || filterKey.equals(""))
                .mapValues(mapValue -> {
                    String a = mapValue.get("current_status").toString().toLowerCase().replace("\"", "");
                    jsonOutputObject.put(a,Integer.parseInt(jsonOutputObject.get(a).toString()) + 1);
                    return jsonOutputObject;
                        });

        outputJsonTable.peek((key, value) -> {
            System.out.println(value);
        });

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static JsonNode getJsonOutput1(String key, JsonNode existingInput){
        key = key.replace("\"", "").toLowerCase();
        System.out.println("key = " + key + ", existingInput = " + existingInput);

        if (key.toLowerCase().equals("migrated") || key.toLowerCase().equals(""))
            return existingInput;

        ObjectNode jsonOutput = new ObjectMapper().createObjectNode();
        jsonOutput = existingInput.deepCopy();
        jsonOutput.put(key.toLowerCase(), Integer.parseInt(jsonOutput.get(key.toLowerCase()).toString().replace("\"", "")) + 1);

        return jsonOutput;
    }

    private static JsonNode getJsonOutput(JsonNode newInput, JsonNode existingInput) throws JsonProcessingException {

        if (newInput.get("current_status").toString().replace("\"", "").equals("Migrated") || newInput.get("current_status").toString().replace("\"", "").equals(""))
            return existingInput;

        ObjectNode jsonOutput = new ObjectMapper().createObjectNode();
        jsonOutput.put("hospitalized", existingInput.get("hospitalized").toString().replace("\"", ""));
        jsonOutput.put("recovered", existingInput.get("recovered").toString().replace("\"", ""));
        jsonOutput.put("deceased", existingInput.get("deceased").toString().replace("\"", ""));

        String newStatus = newInput.get("current_status").toString().toLowerCase().replace("\"", "");
//        System.out.println("New input");
//        System.out.println(newInput);
//        System.out.println("Existing input");
//        System.out.println(existingInput);
//        System.out.println("New status : " + newStatus);
        jsonOutput.put(newStatus, Integer.parseInt(jsonOutput.get(newStatus).toString().replace("\"", "")) + 1);

//        if (newStatus.equals("recovered")){
//            System.out.println(jsonOutput.toString());
//        }

        return jsonOutput;
    }
}
