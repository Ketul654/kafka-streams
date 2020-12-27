package com.ketul.kafka.stream;

import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Take a comma delimited topic of userid,colour
 * Filter out bad data i.e. keep only colour of green,red and blue
 * Get the running count of the favourite colour overall and output this to a topic
 */
public class FavouriteColourStreamApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(FavouriteColourStreamApplication.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamConstants.FAVOURITE_COLOUR_APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, StreamConstants.AUTO_OFFSET_RESET_EARLIEST);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        Set<String> validColours = new HashSet<>();
        validColours.add("red");
        validColours.add("green");
        validColours.add("blue");

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String,String> favouriteColourTable = builder.table(StreamConstants.FAVOURITE_COLOUR_INPUT_TOPIC);
        KTable<String,Long> colourCountTable = favouriteColourTable.filter((clientId, colour) -> validColours.contains(colour.toLowerCase()))
                .groupBy((clientId, colour) -> KeyValue.pair(colour.toLowerCase(),colour))
                .count();

        colourCountTable.toStream().to(StreamConstants.FAVOURITE_COLOUR_OUTPUT_TOPIC);

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();

        LOGGER.info(topology.describe().toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
