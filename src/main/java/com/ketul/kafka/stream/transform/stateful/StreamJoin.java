package com.ketul.kafka.stream.transform.stateful;

import com.ketul.kafka.message.AccountDetails;
import com.ketul.kafka.message.Customer;
import com.ketul.kafka.serde.AccountDetailsDeserializer;
import com.ketul.kafka.serde.AccountDetailsSerializer;
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
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic account-details-input --replication-factor 3 --partitions 3
 * bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic customer-output --replication-factor 3 --partitions 3
 * <p>
 * 2. Start Customer Producer
 *
 * 3. Start Account Details Producer
 * <p>
 * 3. Start this stream. You can also start multiple instances of stream
 */
public class StreamJoin {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamJoin.class);

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
        Serde<AccountDetails> accountDetailsSerdes = Serdes.serdeFrom(new AccountDetailsSerializer(), new AccountDetailsDeserializer());

        KStream<String, Customer> customerKStream = builder.stream(
                StreamConstants.CONSUMER_INPUT_TOPIC,
                Consumed.with(
                        Serdes.String(),
                        customerSerdes
                )
        );
        KStream<String, AccountDetails> accountDetailsKStream = builder.stream(
                StreamConstants.ACCOUNT_DETAILS_INPUT_TOPIC,
                Consumed.with(
                        Serdes.String(),
                        accountDetailsSerdes
                )
        );

        KStream<String, Customer> joinedStream = customerKStream.join(accountDetailsKStream,
                (customer, accountDetails) -> {
                    customer.setAccountDetails(accountDetails);
                    return customer;
                },
                JoinWindows.of(Duration.ofMinutes(5)),
                Joined.with(
                        Serdes.String(),
                        customerSerdes,
                        accountDetailsSerdes
                )
        );
        joinedStream.foreach((customerId, customer) -> LOGGER.info(String.format("%s=>%s", customerId, customer.toString())));
        return builder.build();
    }

    private static Properties getStreamProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamConstants.APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, StreamConstants.AUTO_OFFSET_RESET_EARLIEST);
        //properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        return properties;
    }
}
