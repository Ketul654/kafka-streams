package com.ketul.kafka.stream;

import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * This is kafka stream application to count number of words
 * Create input and out topics
 * Produce messages from kafka console producer to input topic, run this application and consumer messages from output topic using kafka console consumer
 */
public class WordCountStreamApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountStreamApplication.class);

    public static void main(String[] args) {

        Properties properties = getStreamProperties();

        Topology topology = createTopology();

        KafkaStreams streams = new KafkaStreams(topology, properties);

        LOGGER.info("Starting {}", StreamConstants.WORD_COUNT_APPLICATION_ID);
        streams.start();

        /*
         You can visualize topology here : https://zz85.github.io/kafka-streams-viz/
         Topology will help to understand the execution flow
         */
        LOGGER.info(topology.describe().toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static Topology createTopology() {
        /*
        Get a Stream from kafka topic
         */
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> wordCountInput = builder.stream(StreamConstants.WORD_INPUT_TOPIC);

        KTable<String, Long> wordCounts = wordCountInput
                .mapValues(sentence -> {
                    LOGGER.info("Converting sentence \'{}\' to lower case", sentence);
                    /*
                    i.e. if stream = <null,Kafka connect and Kafka Stream>
                    output = <null,kafka connect and kafka stream>
                     */
                    return sentence.toLowerCase();
                })
                .flatMapValues(sentence -> {
                    List<String> words = Arrays.asList(sentence.split(" "));
                    LOGGER.info("Converting \'{}\' to words list {}", sentence, words.toString());
                    /*
                    output = <null,kafka>,<null,connect>,<null,and>,<null,kafka>,<null,stream>
                     */
                    return words;
                })
                .selectKey((keyToChange, word) -> {
                    LOGGER.info("Making key {} same as word {}", keyToChange, word);
                    /*
                    output = <kafka,kafka>,<connect,connect>,<and,and>,<kafka,kafka>,<stream,stream>
                     */
                    return word;
                })
                /*
                output of groupByKey() = (<kafka,kafka>,<kafka,kafka>),(<connect,connect>),(<and,and>),(<stream,stream>)
                 */
                .groupByKey()
                /*
                output count() = <kafka,2>,<connect,1>,<and,1>,<stream,1>
                 */
                .count();

        /*
        Please note that I have added logging intentionally to understand the flow. It is big overhead to log everything in real stream application.
        Below is the equivalent statement without logging.
        KTable<String, Long> wordCounts = wordCountInput
                .mapValues(sentence -> sentence.toLowerCase())
                .flatMapValues(sentence -> Arrays.asList(sentence.split(" ")))
                .selectKey((key, word) -> word)
                .groupByKey()
                .count();
         */

        /*
        Setting output topic to write output to
         */
        wordCounts.toStream().to(StreamConstants.WORD_OUTPUT_TOPIC);

        return builder.build();
    }

    private static Properties getStreamProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamConstants.WORD_COUNT_APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, StreamConstants.AUTO_OFFSET_RESET_EARLIEST);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }
}
