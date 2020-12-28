package com.ketul.kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ketul.kafka.data.BankTransaction;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class BankTransactionDeserializer implements Deserializer<BankTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BankTransactionDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        LOGGER.info("Initializing bank transaction deserializer with configs : {}", configs.toString());
    }

    @Override
    public BankTransaction deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        BankTransaction bankTransaction = null;
        try {
            mapper.registerModule(new JavaTimeModule());
            bankTransaction = mapper.readValue(bytes, BankTransaction.class);
        } catch (IOException e) {
            LOGGER.error("Exception occurred while deserializing bank transaction : ", e);
        }
        return bankTransaction;
    }

    @Override
    public void close() {
        LOGGER.info("Closing bank transaction deserializer");
    }
}
