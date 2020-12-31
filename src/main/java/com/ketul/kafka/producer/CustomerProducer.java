package com.ketul.kafka.producer;

import com.ketul.kafka.message.AccountDetails;
import com.ketul.kafka.message.Customer;
import com.ketul.kafka.serde.CustomerSerializer;
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

public class CustomerProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomerProducer.class);

    public static void main(String[] args) {
        Properties properties = getProducerProperties();
        KafkaProducer<String, Customer> customerKafkaProducer = new KafkaProducer<String, Customer>(properties);
        sendCustomerDetails(customerKafkaProducer);
    }

    private static void sendCustomerDetails(Producer<String, Customer> customerKafkaProducer) {
        try {
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 10; j++) {
                    Customer customer = createRandomCustomer();
                    LOGGER.info(customer.toString());
                    customerKafkaProducer.send(new ProducerRecord<>(StreamConstants.INPUT_TOPIC, customer));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Exception occurred while sending message : ", e);
        } finally {
            customerKafkaProducer.close();
        }
    }

    private static Customer createRandomCustomer() {
        String[] customerIds = new String[]{"ketul123", "bhumi726", "vipul879", "rony235", "umesh457", "piyush1705"};
        String[] names = new String[]{"ketul", "bhumi", "vipul", "rony", "umesh", "piyush"};
        int[] ages = new int[]{22, 17, 23, 25, 35, 27};
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
        String name = names[randomIndex];
        int age = ages[randomIndex];
        float averageAccountBalance = new Random().nextFloat() * (10000 - 1) + 1;
        Instant accountCreationTime = accountCreationTimes[randomIndex];
        long accountNumber = accountNumbers[randomIndex];
        AccountDetails accountDetails = new AccountDetails(customerId, accountNumber, averageAccountBalance, accountCreationTime, Instant.now());
        return new Customer(customerId, name, age, accountDetails);
    }

    private static Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, StreamConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomerSerializer.class.getName());
        return properties;
    }
}
