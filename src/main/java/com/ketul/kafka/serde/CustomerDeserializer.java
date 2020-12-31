package com.ketul.kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ketul.kafka.message.Customer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class CustomerDeserializer implements Deserializer<Customer> {
    private final Logger logger = LoggerFactory.getLogger(CustomerDeserializer.class);
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info("Starting customer deserializer with configs {}", configs.toString());
    }

    @Override
    public Customer deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        Customer customer = null;
        try {
            customer = mapper.readValue(bytes, Customer.class);
        } catch (IOException e) {
            logger.info("Exception occurred while deserializing customer : ", e);
        }
        return customer;
    }

    @Override
    public void close() {
        logger.info("Stopping customer deserializer");
    }
}
