package com.ketul.kafka.stream.transform.stateless;

import com.ketul.kafka.message.Customer;
import com.ketul.kafka.serde.CustomerDeserializer;
import com.ketul.kafka.serde.CustomerSerializer;
import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Topic -> Stream -> Filter -> GroupBy -> Foreach to print stream data
 * <p>
 * 1. Create input and output topic
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-input --replication-factor 3 --partitions 3
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-output --replication-factor 3 --partitions 3
 * <p>
 * 2. Start Customer With Account Details Producer
 * <p>
 * 3. Start this stream. You can also start multiple instances of stream
 */
public class StreamGroupBy {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamGroupBy.class);

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
        Serde<Customer> customerSerdes = Serdes.serdeFrom(new CustomerSerializer(), new CustomerDeserializer());
        KGroupedStream<Long, Customer> kGroupedStream = builder.stream(StreamConstants.CONSUMER_INPUT_TOPIC,
                // Explicitly declaring serdes.
                Consumed.with(
                        Serdes.String(),
                        customerSerdes // Custom sedes
                ))
                .filter(((customerId, customer) -> customer.getAge() >= 25))
                /*
                 * groupBy always triggers re-partition.
                 *
                 * It groups by new key. key can be of new type.
                 *
                 * Sub-topology: 0
                    Source: KSTREAM-SOURCE-0000000000 (topics: [customer-input])
                      --> KSTREAM-FILTER-0000000001
                    Processor: KSTREAM-FILTER-0000000001 (stores: [])
                      --> KSTREAM-KEY-SELECT-0000000002
                      <-- KSTREAM-SOURCE-0000000000
                    Processor: KSTREAM-KEY-SELECT-0000000002 (stores: [])
                      --> KSTREAM-FILTER-0000000006
                      <-- KSTREAM-FILTER-0000000001
                    Processor: KSTREAM-FILTER-0000000006 (stores: [])
                      --> KSTREAM-SINK-0000000005
                      <-- KSTREAM-KEY-SELECT-0000000002
                    Sink: KSTREAM-SINK-0000000005 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition)
                      <-- KSTREAM-FILTER-0000000006

                  Sub-topology: 1
                    Source: KSTREAM-SOURCE-0000000007 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition])
                      --> KSTREAM-AGGREGATE-0000000004
                    Processor: KSTREAM-AGGREGATE-0000000004 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000003])
                      --> KTABLE-TOSTREAM-0000000008
                      <-- KSTREAM-SOURCE-0000000007
                    Processor: KTABLE-TOSTREAM-0000000008 (stores: [])
                      --> KSTREAM-FOREACH-0000000009
                      <-- KSTREAM-AGGREGATE-0000000004
                    Processor: KSTREAM-FOREACH-0000000009 (stores: [])
                      --> none
                      <-- KTABLE-TOSTREAM-0000000008
                 */
                .groupBy(
                        (customerId, customer) -> customer.getAccountDetails().getAccountNumber(),
                        Grouped.with(
                                Serdes.Long(),
                                customerSerdes
                        )
                );
        KTable<Long, Long> countTable = kGroupedStream.count();
        countTable.toStream().foreach((customerId, count) -> LOGGER.info(String.format("%d => %d", customerId, count)));
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
