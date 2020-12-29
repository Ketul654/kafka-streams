package com.ketul.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ketul.kafka.message.BankTransaction;
import com.ketul.kafka.serde.BankTransactionSerializer;
import com.ketul.kafka.stream.BankBalanceStreamApplication;
import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class BankTransactionProducerTest {

    private MockProducer<String, BankTransaction> mockProducer;
    private ObjectMapper mapper = new ObjectMapper();
    @Before
    public void setUp() throws Exception {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new BankTransactionSerializer(mapper));
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