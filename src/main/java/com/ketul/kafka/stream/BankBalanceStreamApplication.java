package com.ketul.kafka.stream;

import com.ketul.kafka.data.BankBalance;
import com.ketul.kafka.data.BankTransaction;
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

public class BankBalanceStreamApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(BankBalanceStreamApplication.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamConstants.FAVOURITE_COLOUR_APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, StreamConstants.AUTO_OFFSET_RESET_EARLIEST);

        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // This simple one line configuration provides exactly once semantics. Magic!!
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        Serde<BankTransaction> bankTransactionSerdes = Serdes.serdeFrom(new BankTransactionSerializer(), new BankTransactionDeserializer());
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, BankTransaction> bankTransactionKStream = builder.stream(StreamConstants.BANK_TRANSACTIONS_TOPIC,
                Consumed.with(
                        Serdes.String(),
                        bankTransactionSerdes
                ));

        KGroupedStream<String, BankTransaction> bankBalanceKStream = bankTransactionKStream.groupByKey();

        Serde bankBalanceSerdes = Serdes.serdeFrom(new BankBalanceSerializer(), new BankBalanceDeserializer());

        KTable<String, BankBalance> bankBalanceKTable =  bankBalanceKStream.aggregate(
                () -> new BankBalance(0, Instant.ofEpochMilli(0L), 0),
                (name, bankTransaction, bankBalance) -> {
                    return new BankBalance(
                        bankTransaction.getAmount() + bankBalance.getBalance(),
                        bankTransaction.getTime().compareTo(bankBalance.getTime()) >= 0 ? bankTransaction.getTime() : bankBalance.getTime(),
                            bankBalance.getNoOfTransaction() + 1
                    );
                },
                Materialized.as("bank-balance").withValueSerde(bankBalanceSerdes).withKeySerde(Serdes.String())
        );

        bankBalanceKTable.toStream().to(StreamConstants.BANK_BALANCES_TOPIC);

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.cleanUp();
        streams.start();

        LOGGER.info(topology.describe().toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
