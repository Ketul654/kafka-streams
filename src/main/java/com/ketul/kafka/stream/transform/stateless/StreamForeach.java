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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Topic -> Stream -> Foreach to print stream data
 * <p>
 * 1. Create input and output topic
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-input --replication-factor 3 --partitions 3
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-output --replication-factor 3 --partitions 3
 * <p>
 * 2. Start Customer Producer
 * <p>
 * 3. Start this stream. You can also start multiple instances of stream
 *
 */
public class StreamForeach {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamForeach.class);

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
        builder.stream(StreamConstants.INPUT_TOPIC,
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
                .foreach(((customerId, customer) -> LOGGER.info(String.format("%s : %s", customerId, customer.getAccountDetails().toString()))));
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
