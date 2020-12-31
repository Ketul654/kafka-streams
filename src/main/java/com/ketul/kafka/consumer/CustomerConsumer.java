package com.ketul.kafka.consumer;

import com.ketul.kafka.message.Customer;
import com.ketul.kafka.serde.CustomerDeserializer;
import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;


public class CustomerConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomerConsumer.class);
    public static void main(String[] args) {
        Properties properties = getConsumerProperties();
        ArrayList<String> topics = new ArrayList<>();
        topics.add(StreamConstants.OUTPUT_TOPIC);
        topics.add(StreamConstants.OUTPUT_COMPACTED_TOPIC);
        topics.add(StreamConstants.K_OUTPUT_TOPIC);
        topics.add(StreamConstants.V_OUTPUT_TOPIC);
        topics.add(StreamConstants.B_OUTPUT_TOPIC);
        topics.add(StreamConstants.P_OUTPUT_TOPIC);
        topics.add(StreamConstants.U_OUTPUT_TOPIC);
        topics.add(StreamConstants.OTHER_OUTPUT_TOPIC);
        KafkaConsumer<String, Customer> customerKafkaConsumer = new KafkaConsumer<String, Customer>(properties);
        consumeCustomerDetails(customerKafkaConsumer, topics);
    }

    private static void consumeCustomerDetails(Consumer<String, Customer> customerKafkaConsumer, ArrayList<String> topics) {
        customerKafkaConsumer.subscribe(topics);
        try {
            while (true) {
                ConsumerRecords<String, Customer> consumerRecords = customerKafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Customer> consumerRecord : consumerRecords) {
                    LOGGER.info(String.format("Topic : %s, Partition : %s, Offset : %s, Key : %s, Value : %s",
                            consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value()));
                }
            }

        } catch (Exception e) {
                LOGGER.error("Exception occurred while consuming customer data : ", e);
        } finally {
            customerKafkaConsumer.close();
        }
    }

    private static Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, StreamConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "customer");
        return properties;
    }
}
