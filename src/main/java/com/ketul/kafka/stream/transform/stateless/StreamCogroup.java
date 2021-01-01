package com.ketul.kafka.stream.transform.stateless;

import com.ketul.kafka.aggregator.TransactionStatusAggregator;
import com.ketul.kafka.message.Customer;
import com.ketul.kafka.message.TransactionStatus;
import com.ketul.kafka.serde.CustomerDeserializer;
import com.ketul.kafka.serde.CustomerSerializer;
import com.ketul.kafka.serde.TransactionStatusDeserializer;
import com.ketul.kafka.serde.TransactionStatusSerializer;
import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * KStream -> KStream[] -> Cogroup -> Aggregate -> Foreach
 *
 * 1. Create input and output topics
 *    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-input --replication-factor 3 --partitions 3
 *    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic other-output --replication-factor 3 --partitions 3
 *
 * 2. Start producer
 *
 * 3. Start this stream
 */
public class StreamCogroup {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamCogroup.class);
    public static void main(String[] args) {
        Properties properties = getStreamProperties();
        Topology topology = createTopology();
        LOGGER.info(topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<Customer> customerSerdes =  Serdes.serdeFrom(new CustomerSerializer(), new CustomerDeserializer());
        KStream<String, Customer> customerKStream = builder.stream(StreamConstants.INPUT_TOPIC,
                // Explicitly declaring serdes.
                Consumed.with(
                        Serdes.String(),
                        customerSerdes // Custom sedes
                ));

        KStream<String, Customer> [] customerKStreams = customerKStream.branch(
                (customerId, customer) -> customerId.toUpperCase().startsWith("K"),
                (customerId, customer) -> customerId.toUpperCase().startsWith("V"),
                (customerId, customer) -> customerId.toUpperCase().startsWith("B"),
                (customerId, customer) -> customerId.toUpperCase().startsWith("P"),
                (customerId, customer) -> customerId.toUpperCase().startsWith("U"),
                (customerId, customer) -> true
        );

        KGroupedStream<String, Customer> streamForKeyStartingWithK = customerKStreams[0].groupByKey(
                Grouped.with(
                        Serdes.String(),
                        customerSerdes
                )
        );

        KGroupedStream<String, Customer> streamForKeyStartingWithB = customerKStreams[2].groupByKey(
                Grouped.with(
                        Serdes.String(),
                        customerSerdes
                )
        );

        CogroupedKStream<String, TransactionStatus> cogroupedKStream = streamForKeyStartingWithK
                .cogroup(new TransactionStatusAggregator())
                .cogroup(streamForKeyStartingWithB, new TransactionStatusAggregator());

        KTable<String, TransactionStatus> statusKTable = cogroupedKStream.
                aggregate(
                        () -> new TransactionStatus(0.0f, 0.0f, 0.0f),
                        Materialized.<String, TransactionStatus, KeyValueStore<Bytes, byte[]>>as("filtered-account-transaction-status-store")
                                .withValueSerde(Serdes.serdeFrom(new TransactionStatusSerializer(), new TransactionStatusDeserializer()))
                                .withKeySerde(Serdes.String())
                );

        statusKTable.toStream().foreach((customerId, transactionStatus) -> LOGGER.info(String.format("%s => %s", customerId, transactionStatus.toString())));

        return builder.build();
    }

    private static Properties getStreamProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamConstants.APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, StreamConstants.AUTO_OFFSET_RESET_EARLIEST);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        return properties;
    }
}
