package com.ketul.kafka.store;

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
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Topic -> Stream -> Filter -> FlatMap -> GroupByKey -> Non Windowed Count -> Foreach to print stream data
 * Query Non Window Store
 * <p>
 * 1. Create input and output topic
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-input --replication-factor 3 --partitions 3
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-output --replication-factor 3 --partitions 3
 * <p>
 * 2. Start Customer With Account Details Producer
 * <p>
 * 3. Start this stream. You can also start multiple instances of stream
 */
public class QueryKeyValueStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryKeyValueStore.class);
    private static final String KEY_VALUE_STORE_NAME = "key-value-store";

    public static void main(String[] args) {
        Properties properties = getStreamProperties();
        Topology topology = createTopology();
        LOGGER.info(topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
        readWindowStoreThroughInteractiveQuery(streams);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void readWindowStoreThroughInteractiveQuery(KafkaStreams streams) {
        StoreQueryParameters<ReadOnlyKeyValueStore<String, Long>> queryParameters = StoreQueryParameters.fromNameAndType(KEY_VALUE_STORE_NAME, QueryableStoreTypes.keyValueStore());
        ReadOnlyKeyValueStore<String, Long> readOnlyKeyValueStore = streams.store(queryParameters);

        while (streams.state().isRunningOrRebalancing()) {
            try {
                KeyValueIterator<String, Long> iterator = readOnlyKeyValueStore.all();

                while (iterator.hasNext()) {
                    KeyValue<String, Long> next = iterator.next();
                    LOGGER.info("count for customer {} is {} in store {}", next.key, next.value, KEY_VALUE_STORE_NAME);
                }
                Thread.sleep(30000);
            } catch (Exception ex) {
                LOGGER.error(ex.getMessage());
            }
        }
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
        This calculates total customer details for change in balance
         */
        KTable<String, Long> aggregatedBalanceTable = kGroupedStream.count(
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(KEY_VALUE_STORE_NAME)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
        );

        aggregatedBalanceTable.toStream().foreach((customerId, transactionStatus) -> LOGGER.info(String.format("%s => %s", customerId, transactionStatus.toString())));
        return builder.build();
    }

    private static Properties getStreamProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamConstants.APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, StreamConstants.AUTO_OFFSET_RESET_EARLIEST);
        return properties;
    }
}
