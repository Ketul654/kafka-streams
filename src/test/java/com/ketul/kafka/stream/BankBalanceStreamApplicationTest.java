package com.ketul.kafka.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ketul.kafka.message.BankBalance;
import com.ketul.kafka.message.BankTransaction;
import com.ketul.kafka.serde.BankBalanceDeserializer;
import com.ketul.kafka.serde.BankBalanceSerializer;
import com.ketul.kafka.serde.BankTransactionDeserializer;
import com.ketul.kafka.serde.BankTransactionSerializer;
import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

public class BankBalanceStreamApplicationTest {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String,BankTransaction> inputTopic;
    private TestOutputTopic<String,BankBalance> outputTopic;
    private ObjectMapper mapper = new ObjectMapper();
    private Serde<BankTransaction> bankTransactionSerde = Serdes.serdeFrom(new BankTransactionSerializer(mapper), new BankTransactionDeserializer(mapper));
    private Serde<BankBalance> bankBalanceSerde = Serdes.serdeFrom(new BankBalanceSerializer(mapper), new BankBalanceDeserializer(mapper));

    @Before
    public void setUp() throws Exception {
        BankBalanceStreamApplication application = new BankBalanceStreamApplication();
        Method method = BankBalanceStreamApplication.class.getDeclaredMethod("createTopology");
        method.setAccessible(true);
        Topology topology = (Topology) method.invoke(application, null);
        method.setAccessible(false);

        method = BankBalanceStreamApplication.class.getDeclaredMethod("getStreamProperties");
        method.setAccessible(true);
        Properties properties = (Properties) method.invoke(application, null);
        method.setAccessible(false);

        topologyTestDriver = new TopologyTestDriver(topology, properties);

        inputTopic = topologyTestDriver.createInputTopic(StreamConstants.BANK_TRANSACTIONS_TOPIC, Serdes.String().serializer(), bankTransactionSerde.serializer());
        outputTopic = topologyTestDriver.createOutputTopic(StreamConstants.BANK_BALANCES_TOPIC, Serdes.String().deserializer(), bankBalanceSerde.deserializer());
    }

    @Test
    public void testBankBalance(){
        inputTopic.pipeInput("ketul", new BankTransaction("ketul", 100, Instant.parse("2019-06-01T10:00:00Z")));
        inputTopic.pipeInput("ketul", new BankTransaction("ketul", 200, Instant.parse("2019-07-01T10:00:00Z")));
        inputTopic.pipeInput("bhumi", new BankTransaction("bhumi", 300, Instant.parse("2019-08-01T10:00:00Z")));
        inputTopic.pipeInput("ketul", new BankTransaction("ketul", 400, Instant.parse("2019-10-01T10:00:01Z")));
        inputTopic.pipeInput("bhumi", new BankTransaction("bhumi", 500, Instant.parse("2019-10-01T10:00:00Z")));

        Map<String, BankBalance> bankBalanceMap = outputTopic.readKeyValuesToMap();
        BankBalance balance = bankBalanceMap.get("ketul");
        Assert.assertEquals(700, balance.getCurrentBalance(), 0);
        Assert.assertEquals(balance.getLastTransactionTime(), Instant.parse("2019-10-01T10:00:01Z"));
        Assert.assertEquals(balance.getTotalTransactions(), 3);

        balance = bankBalanceMap.get("bhumi");
        Assert.assertEquals(800, balance.getCurrentBalance(), 0);
        Assert.assertEquals(balance.getLastTransactionTime(), Instant.parse("2019-10-01T10:00:00Z"));
        Assert.assertEquals(balance.getTotalTransactions(), 2);

        inputTopic.pipeInput("ketul", new BankTransaction("ketul", -400, Instant.parse("2020-10-01T10:30:01Z")));
        inputTopic.pipeInput("bhumi", new BankTransaction("bhumi", -300, Instant.parse("2020-10-01T11:25:00Z")));

        bankBalanceMap = outputTopic.readKeyValuesToMap();
        balance = bankBalanceMap.get("ketul");
        Assert.assertEquals(300, balance.getCurrentBalance(), 0);
        Assert.assertEquals(balance.getLastTransactionTime(), Instant.parse("2020-10-01T10:30:01Z"));
        Assert.assertEquals(balance.getTotalTransactions(), 4);

        balance = bankBalanceMap.get("bhumi");
        Assert.assertEquals(500, balance.getCurrentBalance(), 0);
        Assert.assertEquals(balance.getLastTransactionTime(), Instant.parse("2020-10-01T11:25:00Z"));
        Assert.assertEquals(balance.getTotalTransactions(), 3);
    }

    @After
    public void tearDown() throws Exception {
        topologyTestDriver.close();
    }
}