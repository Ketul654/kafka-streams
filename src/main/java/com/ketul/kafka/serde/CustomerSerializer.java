package com.ketul.kafka.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ketul.kafka.message.Customer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CustomerSerializer implements Serializer<Customer> {
    private final Logger logger = LoggerFactory.getLogger(CustomerSerializer.class);
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info("Starting customer serialization with config : {}", configs.toString());
    }

    @Override
    public byte[] serialize(String s, Customer customer) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        byte[] serializedCustomer = new byte[0];
        try {
            serializedCustomer = mapper.writeValueAsString(customer).getBytes();
        } catch (JsonProcessingException e) {
            logger.error("Exception occurred while serializing customer {} : ", (customer == null) ? "" : customer, e);
        }
        return serializedCustomer;
    }

    @Override
    public void close() {
        logger.info("Stopping customer serialization");
    }
}
