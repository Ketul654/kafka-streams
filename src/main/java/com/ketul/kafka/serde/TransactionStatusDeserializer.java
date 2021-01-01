package com.ketul.kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ketul.kafka.message.TransactionStatus;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class TransactionStatusDeserializer implements Deserializer<TransactionStatus> {

    private final Logger logger = LoggerFactory.getLogger(TransactionStatusDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info("Starting transaction status serialization with config : {}", configs.toString());
    }

    @Override
    public TransactionStatus deserialize(String topic, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        TransactionStatus transactionStatus = null;
        try {
            transactionStatus = mapper.readValue(bytes, TransactionStatus.class);
        } catch (IOException e) {
            logger.info("Exception occurred while deserializing transaction status : ", e);
        }
        return transactionStatus;
    }

    @Override
    public void close() {
        logger.info("Stopping transaction status deserializer");
    }
}
