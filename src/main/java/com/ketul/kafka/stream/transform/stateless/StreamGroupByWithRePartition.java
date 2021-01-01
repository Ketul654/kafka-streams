package com.ketul.kafka.stream.transform.stateless;

import com.ketul.kafka.message.AccountDetails;
import com.ketul.kafka.message.Customer;
import com.ketul.kafka.serde.CustomerDeserializer;
import com.ketul.kafka.serde.CustomerSerializer;
import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Topic -> Stream -> Filter -> FlatMap -> GroupByKey -> Foreach to print stream data
 * <p>
 * 1. Create input and output topic
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-input --replication-factor 3 --partitions 3
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-output --replication-factor 3 --partitions 3
 * <p>
 * 2. Start Customer Producer
 * <p>
 * 3. Start this stream. You can also start multiple instances of stream
 */
public class StreamGroupByWithRePartition {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamGroupByWithRePartition.class);

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
        KGroupedStream<String, Customer> kGroupedStream = builder.stream(StreamConstants.INPUT_TOPIC,
                // Explicitly declaring serdes.
                Consumed.with(
                        Serdes.String(),
                        customerSerdes // Custom sedes
                ))
                .filter(((customerId, customer) -> customer.getAge() >= 25))
                .filterNot(((customerId, customer) -> customerId.startsWith("u")))
                /*
                This will mark stream data for re-partitioning. Applying group by or join after this triggers re-partitioning.
                That is the reason flatMapValues is preferred over flatMap.
                 */
                .flatMap(((customerId, customer) -> {
                    List<KeyValue<String, Customer>> customerList = new ArrayList<>();
                    customerList.add(KeyValue.pair(customerId.toLowerCase(), customer));
                    customerList.add(KeyValue.pair(customerId.toUpperCase(), customer));
                    return customerList;
                }))
                .flatMapValues((Customer customer) -> {
                    AccountDetails dummyAccountDetails = new AccountDetails(customer.getCustomerId(), 11111111111L, 0.0f, Instant.now(), Instant.now());
                    List<Customer> customerList = new ArrayList<>();
                    customerList.add(customer);
                    customerList.add(new Customer(customer.getCustomerId(), customer.getName(), customer.getAge(), dummyAccountDetails));
                    return customerList;
                })
                /*
                * groupByKey will trigger re-partition as stream is already marked for it by flatMap.
                * It will not trigger repartition if stream is not marked for it.
                *
                * It groups the record with existing key
                *
                *    Sub-topology: 0
                        Source: KSTREAM-SOURCE-0000000000 (topics: [customer-input])
                          --> KSTREAM-FILTER-0000000001
                        Processor: KSTREAM-FILTER-0000000001 (stores: [])
                          --> KSTREAM-FILTER-0000000002
                          <-- KSTREAM-SOURCE-0000000000
                        Processor: KSTREAM-FILTER-0000000002 (stores: [])
                          --> KSTREAM-FLATMAP-0000000003
                          <-- KSTREAM-FILTER-0000000001
                        Processor: KSTREAM-FLATMAP-0000000003 (stores: [])
                          --> KSTREAM-FLATMAPVALUES-0000000004
                          <-- KSTREAM-FILTER-0000000002
                        Processor: KSTREAM-FLATMAPVALUES-0000000004 (stores: [])
                          --> KSTREAM-FILTER-0000000008
                          <-- KSTREAM-FLATMAP-0000000003
                        Processor: KSTREAM-FILTER-0000000008 (stores: [])
                          --> KSTREAM-SINK-0000000007
                          <-- KSTREAM-FLATMAPVALUES-0000000004
                        Sink: KSTREAM-SINK-0000000007 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition)
                          <-- KSTREAM-FILTER-0000000008

                      Sub-topology: 1
                        Source: KSTREAM-SOURCE-0000000009 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition])
                          --> KSTREAM-AGGREGATE-0000000006
                        Processor: KSTREAM-AGGREGATE-0000000006 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000005])
                          --> KTABLE-TOSTREAM-0000000010
                          <-- KSTREAM-SOURCE-0000000009
                        Processor: KTABLE-TOSTREAM-0000000010 (stores: [])
                          --> KSTREAM-SINK-0000000011
                          <-- KSTREAM-AGGREGATE-0000000006
                        Sink: KSTREAM-SINK-0000000011 (topic: customer-compacted-output)
                          <-- KTABLE-TOSTREAM-0000000010
                 */
                .groupByKey(
                        Grouped.with(
                                Serdes.String(),
                                customerSerdes
                        )
                );
        KTable<String, Long> countTable = kGroupedStream.count();
        countTable.toStream().foreach((customerId, count) -> LOGGER.info(String.format("%s => %d", customerId, count)));
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