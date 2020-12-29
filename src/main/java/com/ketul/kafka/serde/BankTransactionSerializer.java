package com.ketul.kafka.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ketul.kafka.message.BankTransaction;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BankTransactionSerializer implements Serializer<BankTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BankTransactionSerializer.class);
    private ObjectMapper mapper;

    public BankTransactionSerializer(ObjectMapper mapper){
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.mapper = mapper;
    }

    @Override
    public void configure(Map configs, boolean isKey) {
        LOGGER.info("Starting bank transaction serializer with configs {}", configs.toString());
    }

    @Override
    public byte[] serialize(String topic, BankTransaction bankTransaction) {
        byte [] bankTransactionBytes = null;
        try {
            bankTransactionBytes = mapper.writeValueAsString(bankTransaction).getBytes();
        } catch (JsonProcessingException e) {
            LOGGER.error("Exception occurred while serializing bank transaction {} : ", bankTransaction, e);
        }
        return bankTransactionBytes;
    }

    @Override
    public void close() {
        LOGGER.info("Closing bank transaction serializer");
    }
}
