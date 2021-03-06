package com.ketul.kafka.stream.transform.stateful;

import com.ketul.kafka.message.Customer;
import com.ketul.kafka.serde.CustomerDeserializer;
import com.ketul.kafka.serde.CustomerSerializer;
import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Topic -> Stream -> Filter -> FlatMap -> GroupByKey -> Windowed Count -> Foreach to print stream data
 * <p>
 * 1. Create input and output topic
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-input --replication-factor 3 --partitions 3
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-output --replication-factor 3 --partitions 3
 * <p>
 * 2. Start Customer With Account Details Producer
 * <p>
 * 3. Start this stream. You can also start multiple instances of stream
 */
public class StreamWindowedCount {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamWindowedCount.class);

    public static void main(String[] args) {
        Properties properties = getStreamProperties();
        Topology topology = createTopology();
        LOGGER.info(topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<Customer> customerSerdes = Serdes.serdeFrom(new CustomerSerializer(), new CustomerDeserializer());
        KGroupedStream<String, Customer> kGroupedStream = builder.stream(StreamConstants.CONSUMER_INPUT_TOPIC,
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
                /*
                 * groupByKey will trigger re-partition as stream is already marked for it by flatMap.
                 * It will not trigger repartition if stream is not marked for it.
                 *
                 * It groups the record with existing key
                 *
                 *
                 */
                .groupByKey(
                        Grouped.with(
                                Serdes.String(),
                                customerSerdes
                        )
                );

        /*
        This calculates total customer details for change in balance by window
         */
        KTable<Windowed<String>, Long> aggregatedBalanceTable = kGroupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(500)).grace(Duration.ofMillis(10)))
                .count(
                        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("windowed-count-store")
                                .withValueSerde(Serdes.Long())
                                .withKeySerde(Serdes.String()).withRetention(Duration.ofMillis(1000))
                );

        aggregatedBalanceTable.toStream().foreach(
                (customerId, count) -> LOGGER.info(String.format("[%s@%s-%s] => %d", customerId.key(), customerId.window().startTime(), customerId.window().endTime(), count)));
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
