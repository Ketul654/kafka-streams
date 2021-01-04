package com.ketul.kafka.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ketul.kafka.message.AccountDetails;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class AccountDetailsSerializer implements Serializer<AccountDetails> {
    private final Logger logger = LoggerFactory.getLogger(AccountDetailsSerializer.class);
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info("Starting account details serialization with config : {}", configs.toString());
    }

    @Override
    public byte[] serialize(String s, AccountDetails accountDetails) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        byte[] serializedAccountDetails = new byte[0];
        try {
            serializedAccountDetails = mapper.writeValueAsString(accountDetails).getBytes();
        } catch (JsonProcessingException e) {
            logger.error("Exception occurred while serializing account details {} : ", (accountDetails == null) ? "" : accountDetails, e);
        }
        return serializedAccountDetails;
    }

    @Override
    public void close() {
        logger.info("Stopping account details serialization");
    }
}
