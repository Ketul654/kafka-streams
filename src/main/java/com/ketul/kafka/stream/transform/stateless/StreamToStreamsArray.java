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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * KStream -> KStream[]
 *
 * 1. Create input and output topics
 *    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-input --replication-factor 3 --partitions 3
 *    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic k-output --replication-factor 3 --partitions 3
 *    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic b-output --replication-factor 3 --partitions 3
 *    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic p-output --replication-factor 3 --partitions 3
 *    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic u-output --replication-factor 3 --partitions 3
 *    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic other-output --replication-factor 3 --partitions 3
 *
 * 2. Start producer
 *
 * 3. Start this stream
 *
 * 4. Start consumer
 */
public class StreamToStreamsArray {
    private static final Logger logger = LoggerFactory.getLogger(StreamToStreamsArray.class);
    public static void main(String[] args) {
        Properties properties = getStreamProperties();
        Topology topology = createTopology();
        logger.info(topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.cleanUp();
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

        customerKStreams[0].to(StreamConstants.K_OUTPUT_TOPIC);
        customerKStreams[1].to(StreamConstants.V_OUTPUT_TOPIC);
        customerKStreams[2].to(StreamConstants.B_OUTPUT_TOPIC);
        customerKStreams[3].to(StreamConstants.P_OUTPUT_TOPIC);
        customerKStreams[4].to(StreamConstants.U_OUTPUT_TOPIC);
        customerKStreams[5].to(StreamConstants.OTHER_OUTPUT_TOPIC);

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
