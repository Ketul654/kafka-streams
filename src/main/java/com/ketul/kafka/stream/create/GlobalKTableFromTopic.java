package com.ketul.kafka.stream.create;

import com.ketul.kafka.message.Customer;
import com.ketul.kafka.serde.CustomerDeserializer;
import com.ketul.kafka.serde.CustomerSerializer;
import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
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
 * Topic -> GlobalKTable -> Topic
 *
 * 1. Create input and output topic
 *    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-input --replication-factor 3 --partitions 3
 *    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-intermediate --replication-factor 3 --partitions 3
 *    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-compacted-output --partitions 3 --replication-factor 3 --config "cleanup.policy=compact" --config "delete.retention.ms=100" --config "segment.ms=100" --config "min.cleanable.dirty.ratio=0.01" --config "segment.bytes=300"
 *    NOTE : these segment configurations for compacted are just for learning purpose and not ideal for production.
 *
 * 2. Start Customer Producer
 *
 * 3. Start this stream
 *
 * 4. Start Customer Consumer after few minutes to let compaction happen so that it will have less number of messages to consume.
 *    You can check how many messages are there on compacted topic using console consumer as well
 *    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic customer-compacted-output --from-beginning
 */
public class GlobalKTableFromTopic {
    private static final Logger logger = LoggerFactory.getLogger(GlobalKTableFromTopic.class);
    public static void main(String[] args) {
        Properties properties = getStreamProperties();
        Topology topology = createTopology();
        logger.info(topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
    }

    private static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<Customer> customerSerdes =  Serdes.serdeFrom(new CustomerSerializer(), new CustomerDeserializer());

        KStream<String, Customer> customerKStream = builder.stream(StreamConstants.INPUT_TOPIC,
                // Explicitly declaring serdes.
                Consumed.with(
                        Serdes.String(),
                        customerSerdes // Custom sedes
                ))
                .filter((customerId, customer) -> customerId.contains("ketul123"))
                .mapValues(customer -> {
                    customer.setAge(20);
                    return customer;
                });

        customerKStream.to(StreamConstants.INTERMEDIATE_TOPIC, Produced.with(
                Serdes.String(),
                customerSerdes
        ));

        GlobalKTable<String, Customer> globalKTable = builder.globalTable(
                StreamConstants.INTERMEDIATE_TOPIC,
                Materialized.<String, Customer, KeyValueStore<Bytes, byte[]>>as("customer-store")
                        .withValueSerde(customerSerdes)
                        .withKeySerde(Serdes.String())
        );

        KStream<String, Customer> joinedKStream = customerKStream.join(
                globalKTable,
                (customerId, customer) -> customerId,
                (customerLeft, customerRight) -> new Customer(customerLeft.getCustomerId(),
                        customerLeft.getName().toUpperCase(),
                        customerRight.getAge(),
                        customerLeft.getAccountDetails())
        );

        joinedKStream.to(StreamConstants.OUTPUT_COMPACTED_TOPIC, Produced.with(
                Serdes.String(),
                customerSerdes
        ));

        return builder.build();

    }

    private static Properties getStreamProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamConstants.APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamConstants.BOOTSTRAP_SERVERS);

        /*
        Reducing batch size less than segment size to avoid failure
         */
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, StreamConstants.AUTO_OFFSET_RESET_EARLIEST);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        return properties;
    }
}
