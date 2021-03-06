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
import org.apache.kafka.streams.kstream.Repartitioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Topic -> Stream -> Repartitiom -> Foreach
 *
 * 1. Create input and output topic
 *    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-input --replication-factor 3 --partitions 3
 *    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-output --replication-factor 3 --partitions 3
 *
 * 2. Start Customer With Account Details Producer
 *
 * 3. Start this stream
 *
 */
public class StreamRepartition {
    private static final Logger logger = LoggerFactory.getLogger(StreamRepartition.class);
    public static void main(String[] args) {
        Properties properties = getStreamProperties();
        Topology topology = createTopology();
        logger.info(topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, properties);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<Customer> customerSerdes =  Serdes.serdeFrom(new CustomerSerializer(), new CustomerDeserializer());
        KStream<String, Customer> customerKStream = builder.stream(StreamConstants.CONSUMER_INPUT_TOPIC,
                // Explicitly declaring serdes.
                Consumed.with(
                        Serdes.String(),
                        customerSerdes // Custom sedes
                ));
        /*
         * Repartitioning input stream
         *
         * bin/kafka-topics.sh --zookeeper localhost:2181 --list | grep -i repartition --color
            customer-stream-application-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition
            customer-stream-application-KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition
            customer-stream-application-KSTREAM-REPARTITION-0000000001-repartition
         *
         *
         * bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic customer-stream-application-KSTREAM-REPARTITION-0000000001-repartition
                Topic: customer-stream-application-KSTREAM-REPARTITION-0000000001-repartition	PartitionCount: 10	ReplicationFactor: 1	Configs: cleanup.policy=delete,message.timestamp.type=CreateTime,segment.bytes=52428800,retention.ms=-1
                    Topic: customer-stream-application-KSTREAM-REPARTITION-0000000001-repartition	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
                    Topic: customer-stream-application-KSTREAM-REPARTITION-0000000001-repartition	Partition: 1	Leader: 0	Replicas: 0	Isr: 0
                    Topic: customer-stream-application-KSTREAM-REPARTITION-0000000001-repartition	Partition: 2	Leader: 2	Replicas: 2	Isr: 2
                    Topic: customer-stream-application-KSTREAM-REPARTITION-0000000001-repartition	Partition: 3	Leader: 1	Replicas: 1	Isr: 1
                    Topic: customer-stream-application-KSTREAM-REPARTITION-0000000001-repartition	Partition: 4	Leader: 0	Replicas: 0	Isr: 0
                    Topic: customer-stream-application-KSTREAM-REPARTITION-0000000001-repartition	Partition: 5	Leader: 2	Replicas: 2	Isr: 2
                    Topic: customer-stream-application-KSTREAM-REPARTITION-0000000001-repartition	Partition: 6	Leader: 1	Replicas: 1	Isr: 1
                    Topic: customer-stream-application-KSTREAM-REPARTITION-0000000001-repartition	Partition: 7	Leader: 0	Replicas: 0	Isr: 0
                    Topic: customer-stream-application-KSTREAM-REPARTITION-0000000001-repartition	Partition: 8	Leader: 2	Replicas: 2	Isr: 2
                    Topic: customer-stream-application-KSTREAM-REPARTITION-0000000001-repartition	Partition: 9	Leader: 1	Replicas: 1	Isr: 1
         *
         * Sub-topology: 0
            Source: KSTREAM-SOURCE-0000000000 (topics: [customer-input])
              --> KSTREAM-FILTER-0000000003
            Processor: KSTREAM-FILTER-0000000003 (stores: [])
              --> KSTREAM-SINK-0000000002
              <-- KSTREAM-SOURCE-0000000000
            Sink: KSTREAM-SINK-0000000002 (topic: KSTREAM-REPARTITION-0000000001-repartition)
              <-- KSTREAM-FILTER-0000000003

          Sub-topology: 1
            Source: KSTREAM-SOURCE-0000000004 (topics: [KSTREAM-REPARTITION-0000000001-repartition])
              --> KSTREAM-FOREACH-0000000005
            Processor: KSTREAM-FOREACH-0000000005 (stores: [])
              --> none
              <-- KSTREAM-SOURCE-0000000004
         */
        KStream<String, Customer> repartitionedStream = customerKStream.repartition(Repartitioned.numberOfPartitions(10));
        repartitionedStream.foreach(((customerId, customer) -> logger.info(String.format("%s : %s", customerId, customer.getAccountDetails().toString()))));
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
