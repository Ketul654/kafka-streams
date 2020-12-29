package com.ketul.kafka.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.ketul.kafka.message.BankBalance;
import com.ketul.kafka.message.BankTransaction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Map;

public class BankBalanceSerdeTest {

    private ObjectMapper mapper = new ObjectMapper();
    private BankBalanceSerializer serializer;
    private BankBalanceDeserializer deserializer;
    private Map config = Mockito.mock(Map.class);
    private String topic = "topic-0";

    @Before
    public void setUp() throws Exception {
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, true);
        serializer = new BankBalanceSerializer(mapper);
        deserializer = new BankBalanceDeserializer(mapper);
        serializer.configure(config, true);
        deserializer.configure(config, true);
    }

    @Test
    public void testValidBankBalance(){
        Instant now = Instant.now();
        byte[] bytes = serializer.serialize(topic, new BankBalance(100f, now, 10));
        BankBalance balance = deserializer.deserialize(topic, bytes);
        Assert.assertEquals(balance.getLastTransactionTime(), now);
        Assert.assertTrue(balance.getCurrentBalance() == 100f);
        Assert.assertTrue(balance.getTotalTransactions() == 10);
    }

    @Test
    public void testDeserializerIOException() {
        /*
         This will throw exception of type or child of IOException which is handled in deserialize method
         */
        BankBalance bankBalance = deserializer.deserialize(topic, new byte[0]);
        Assert.assertNull(bankBalance);
    }

    @After
    public void tearDown() throws Exception {
        serializer.close();
        deserializer.close();
    }
}