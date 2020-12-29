package com.ketul.kafka.producer;

import com.ketul.kafka.message.BankTransaction;
import com.ketul.kafka.serde.BankTransactionSerializer;
import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class BankTransactionProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(BankTransactionProducer.class);

    public static void main(String[] args) {
        Properties properties = getProducerProperties();
        KafkaProducer<String, BankTransaction> producer = new KafkaProducer<String, BankTransaction>(properties);
        produceTransactions(producer, true);
    }

    private static void produceTransactions(Producer<String, BankTransaction> producer, boolean produceContinuously) {
        if (produceContinuously)
            while (true)
                sendMessages(producer);
        else
            sendMessages(producer);
    }

    private static void sendMessages(Producer<String, BankTransaction> producer) {
        String[] customers = new String[]{"John", "Thomas", "Ketul", "Jacob", "Bhumika", "Vipul"};

        Random random = new Random();
        try {
            for (int i = 0; i < 10000; i++) {
                String customer = customers[random.nextInt(customers.length)];
                float amount = new Random().nextFloat() * (999 - 1) + 1;
                BankTransaction bankTransaction = new BankTransaction(customer, amount, Instant.now());
                LOGGER.info(bankTransaction.toString());
                producer.send(new ProducerRecord<>(StreamConstants.BANK_TRANSACTIONS_TOPIC, customer, new BankTransaction(customer, amount, Instant.now())));
            }
            Thread.sleep(1);
        } catch (Exception e) {
            LOGGER.error("Exception occurred while producing messages : ", e);
        } finally {
            producer.close();
        }
    }

    private static Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, StreamConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BankTransactionSerializer.class.getName());

        // Make it idempotence
        properties.put(ProducerConfig.ACKS_CONFIG, StreamConstants.ALL_ACKS);
        properties.put(ProducerConfig.RETRIES_CONFIG, StreamConstants.RETRIES);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, StreamConstants.ENABLE_IDEMPOTENCE);

        // Publish messages very fast
        properties.put(ProducerConfig.LINGER_MS_CONFIG, StreamConstants.LINGER_MS);

        return properties;
    }
}
