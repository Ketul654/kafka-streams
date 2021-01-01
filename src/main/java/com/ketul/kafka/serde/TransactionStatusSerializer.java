package com.ketul.kafka.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ketul.kafka.message.TransactionStatus;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TransactionStatusSerializer implements Serializer<TransactionStatus> {

    private final Logger logger = LoggerFactory.getLogger(TransactionStatusSerializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info("Starting transaction status serializer with configs {} ", configs.toString());
    }

    @Override
    public byte[] serialize(String topic, TransactionStatus transactionStatus) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        byte[] serializedTransactionStatus = new byte[0];
        try {
            serializedTransactionStatus = mapper.writeValueAsString(transactionStatus).getBytes();
        } catch (JsonProcessingException e) {
            logger.error("Exception occurred while serializing transaction status {} : ", (transactionStatus == null) ? "" : transactionStatus, e);
        }
        return serializedTransactionStatus;
    }

    @Override
    public void close() {
        logger.info("Closing transaction status serializer");
    }
}
