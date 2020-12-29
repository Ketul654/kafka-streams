package com.ketul.kafka.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ketul.kafka.message.BankBalance;
import com.ketul.kafka.message.BankTransaction;
import com.ketul.kafka.serde.BankBalanceDeserializer;
import com.ketul.kafka.serde.BankBalanceSerializer;
import com.ketul.kafka.serde.BankTransactionDeserializer;
import com.ketul.kafka.serde.BankTransactionSerializer;
import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;

/**
 *
 * 1. Create input and output topics
 *    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bank-transactions
 *    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bank-balances --config cleanup.policy=compact
 *
 * 2. Star console consumer on input and output topics
 *
 * 3. Make sure you configure producer and consumer to achieve exactly once semantics otherwise it will end up calculating wrong bank balance due to duplication.
 *    i.e. Make producer idempotent and configure processing.guarantee to exactly_once for stream application
 *
 * 4. Start BankTransactionProducer to produce random bank transactions for 6 users to bank-transactions topic
 *
 * 5. Start BankBalanceStreamApplication to calculate total balance, total number of transactions and latest transaction time and publish it to output topic bank-balances
 *
 *
 */
public class BankBalanceStreamApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(BankBalanceStreamApplication.class);

    public static void main(String[] args) {

        Properties properties = getStreamProperties();
        Topology topology = createTopology();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.cleanUp();
        streams.start();

        LOGGER.info(topology.describe().toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Topology createTopology() {
        ObjectMapper mapper = new ObjectMapper();
        Serde<BankTransaction> bankTransactionSerdes = Serdes.serdeFrom(new BankTransactionSerializer(mapper), new BankTransactionDeserializer(mapper));
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, BankTransaction> bankTransactionKStream = builder.stream(StreamConstants.BANK_TRANSACTIONS_TOPIC,
                Consumed.with(
                        Serdes.String(),
                        bankTransactionSerdes
                ));

        Serde bankBalanceSerdes = Serdes.serdeFrom(new BankBalanceSerializer(mapper), new BankBalanceDeserializer(mapper));

        KTable<String, BankBalance> bankBalanceKTable =  bankTransactionKStream.groupByKey().aggregate(
                () -> new BankBalance(0, Instant.ofEpochMilli(0L), 0),
                (name, bankTransaction, bankBalance) -> {
                    return new BankBalance(
                            bankTransaction.getAmount() + bankBalance.getCurrentBalance(),
                            bankTransaction.getTime().compareTo(bankBalance.getLastTransactionTime()) >= 0 ? bankTransaction.getTime() : bankBalance.getLastTransactionTime(),
                            bankBalance.getTotalTransactions() + 1
                    );
                },
                Materialized.as("bank-balance").withValueSerde(bankBalanceSerdes).withKeySerde(Serdes.String())
        );

        bankBalanceKTable.toStream().to(StreamConstants.BANK_BALANCES_TOPIC);

        return builder.build();
    }

    private static Properties getStreamProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamConstants.FAVOURITE_COLOUR_APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, StreamConstants.AUTO_OFFSET_RESET_EARLIEST);

        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5);

        // This simple one line configuration provides exactly once semantics. Magic!!
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        return properties;
    }
}
