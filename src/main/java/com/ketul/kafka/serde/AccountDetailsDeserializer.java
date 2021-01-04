package com.ketul.kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ketul.kafka.message.AccountDetails;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class AccountDetailsDeserializer implements Deserializer<AccountDetails> {
    private final Logger logger = LoggerFactory.getLogger(AccountDetailsDeserializer.class);
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info("Starting account details deserializer with configs {}", configs.toString());
    }

    @Override
    public AccountDetails deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        AccountDetails accountDetails = null;
        try {
            accountDetails = mapper.readValue(bytes, AccountDetails.class);
        } catch (IOException e) {
            logger.info("Exception occurred while deserializing account details : ", e);
        }
        return accountDetails;
    }

    @Override
    public void close() {
        logger.info("Stopping account details deserializer");
    }
}
