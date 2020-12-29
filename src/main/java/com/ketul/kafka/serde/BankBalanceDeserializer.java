package com.ketul.kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ketul.kafka.message.BankBalance;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class BankBalanceDeserializer implements Deserializer<BankBalance> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BankBalanceDeserializer.class);
    private ObjectMapper mapper;

    public BankBalanceDeserializer(ObjectMapper mapper) {
        mapper.registerModule(new JavaTimeModule());
        this.mapper = mapper;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        LOGGER.info("Initializing bank balance deserializer with configs : {}", configs.toString());
    }

    @Override
    public BankBalance deserialize(String topic, byte[] bytes) {
        BankBalance bankBalance = null;
        try {
            bankBalance = mapper.readValue(bytes, BankBalance.class);
        } catch (IOException e) {
            LOGGER.error("Exception occurred while deserializing bank balance : ", e);
        }
        return bankBalance;
    }

    @Override
    public void close() {
        LOGGER.info("Closing bank balance deserializer");
    }
}
