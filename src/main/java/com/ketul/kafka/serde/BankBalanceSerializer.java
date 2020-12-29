package com.ketul.kafka.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ketul.kafka.message.BankBalance;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class BankBalanceSerializer implements Serializer<BankBalance> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BankBalanceSerializer.class);
    private ObjectMapper mapper;

    public BankBalanceSerializer(ObjectMapper mapper) {
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.mapper = mapper;
    }

    @Override
    public void configure(Map configs, boolean isKey) {
        LOGGER.info("Starting bank balance serializer with configs {}", configs.toString());
    }

    @Override
    public byte[] serialize(String topic, BankBalance bankBalance) {
        byte [] bankBalancesBytes = null;
        try {
            bankBalancesBytes = mapper.writeValueAsString(bankBalance).getBytes();
        } catch (JsonProcessingException e) {
            LOGGER.error("Exception occurred while serializing bank balance {} : ", bankBalance, e);
        }
        return bankBalancesBytes;
    }

    @Override
    public void close() {
        LOGGER.info("Closing bank balance serializer");
    }
}
