package com.ketul.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ketul.kafka.message.BankTransaction;
import com.ketul.kafka.serde.BankTransactionSerializer;
import com.ketul.kafka.stream.BankBalanceStreamApplication;
import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class BankTransactionProducerTest {

    private BankTransactionProducer bankTransactionProducer = new BankTransactionProducer();
    private MockProducer<String, BankTransaction> mockProducer;
    private ObjectMapper mapper = new ObjectMapper();
    private String[] customers = new String[]{"John", "Thomas", "Ketul", "Jacob", "Bhumika", "Vipul"};

    @Before
    public void setUp() throws Exception {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new BankTransactionSerializer(mapper));
    }

    @Test
    public void testProducer() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = BankTransactionProducer.class.getDeclaredMethod("produceTransactions", Producer.class, boolean.class);
        method.setAccessible(true);
        method.invoke(bankTransactionProducer, mockProducer, false);

        List<ProducerRecord<String, BankTransaction>> producedTransactions = mockProducer.history();

        Assert.assertEquals(producedTransactions.size(), 10000);

        for(ProducerRecord<String, BankTransaction> producedTransaction : producedTransactions) {
            Assert.assertTrue(Arrays.asList(customers).contains(producedTransaction.key()));
            Assert.assertTrue(producedTransaction.value().getName().equals(producedTransaction.key()));
            Assert.assertTrue(producedTransaction.value().getAmount() < 1000f && producedTransaction.value().getAmount() > 0);
            Assert.assertTrue(!(producedTransaction.value().getTime().compareTo(Instant.now()) == 1));
        }

    }

    @Test
    public void testIfProducerIsIdempotent() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        BankTransactionProducer application = new BankTransactionProducer();
        Method method = BankTransactionProducer.class.getDeclaredMethod("getProducerProperties");
        method.setAccessible(true);
        Properties properties = (Properties) method.invoke(application, null);
        method.setAccessible(false);

        Assert.assertEquals(properties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG) ,true);
        Assert.assertNotEquals(properties.get(ProducerConfig.RETRIES_CONFIG), 0);
        Assert.assertEquals(properties.get(ProducerConfig.ACKS_CONFIG), "all");
    }

    @After
    public void tearDown() throws Exception {
    }
}