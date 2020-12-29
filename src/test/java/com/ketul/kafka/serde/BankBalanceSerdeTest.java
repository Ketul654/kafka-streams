package com.ketul.kafka.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ketul.kafka.message.BankBalance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Map;

public class BankBalanceSerdeTest {

    private ObjectMapper mapper = new ObjectMapper();
    private BankBalanceSerializer serializer = new BankBalanceSerializer(mapper);
    private BankBalanceDeserializer deserializer = new BankBalanceDeserializer(mapper);
    private Map config = Mockito.mock(Map.class);

    @Before
    public void setUp() throws Exception {
        serializer.configure(config, true);
        deserializer.configure(config, true);
    }

    @Test
    public void testValidBankBalance(){
        Instant now = Instant.now();
        byte[] bytes = serializer.serialize("topic-0", new BankBalance(100f, now, 10));
        BankBalance balance = deserializer.deserialize("topic-0", bytes);
        Assert.assertEquals(balance.getLastTransactionTime(), now);
        Assert.assertTrue(balance.getCurrentBalance() ==100f);
        Assert.assertTrue(balance.getTotalTransactions() == 10);
    }

    @After
    public void tearDown() throws Exception {
        serializer.close();
        deserializer.close();
    }
}