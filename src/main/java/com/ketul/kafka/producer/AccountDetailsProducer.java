package com.ketul.kafka.producer;

import com.ketul.kafka.message.AccountDetails;
import com.ketul.kafka.serde.AccountDetailsSerializer;
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

public class AccountDetailsProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccountDetailsProducer.class);

    public static void main(String[] args) {
        Properties properties = getProducerProperties();
        KafkaProducer<String, AccountDetails> customerKafkaProducer = new KafkaProducer<String, AccountDetails>(properties);
        sendAccountDetails(customerKafkaProducer);
    }

    private static void sendAccountDetails(Producer<String, AccountDetails> accountDetailsKafkaProducer) {
        try {
            for (int i = 0; i < 1; i++) {
                for (int j = 0; j < 6; j++) {
                    AccountDetails accountDetails = createRandomAccountDetails();
                    LOGGER.info(accountDetails.toString());
                    accountDetailsKafkaProducer.send(new ProducerRecord<>(StreamConstants.ACCOUNT_DETAILS_INPUT_TOPIC, accountDetails.getCustomerId(), accountDetails));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Exception occurred while sending message : ", e);
        } finally {
            accountDetailsKafkaProducer.close();
        }
    }

    private static AccountDetails createRandomAccountDetails() {
        String[] customerIds = new String[]{"ketul123", "bhumi726", "vipul879", "rony235", "umesh457", "piyush1705"};
        long[] accountNumbers = new long[]{1001000200213L, 1001000200214L, 1001000200215L, 1001000200216L, 1001000200217L, 1001000200218L};
        Instant[] accountCreationTimes = new Instant[]{Instant.parse("2015-10-01T10:30:01Z"),
                Instant.parse("2016-10-01T10:30:01Z"),
                Instant.parse("2017-10-01T10:30:01Z"),
                Instant.parse("2018-10-01T10:30:01Z"),
                Instant.parse("2019-10-01T10:30:01Z"),
                Instant.parse("2020-10-01T10:30:01Z")};
        Random random = new Random();
        int randomIndex = random.nextInt(customerIds.length);
        String customerId = customerIds[randomIndex];
        float averageAccountBalance = new Random().nextFloat() * (10000 - 1) + 1;
        Instant accountCreationTime = accountCreationTimes[randomIndex];
        long accountNumber = accountNumbers[randomIndex];
        return new AccountDetails(customerId, accountNumber, averageAccountBalance, accountCreationTime, Instant.now());
    }

    private static Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, StreamConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AccountDetailsSerializer.class.getName());
        return properties;
    }
}
