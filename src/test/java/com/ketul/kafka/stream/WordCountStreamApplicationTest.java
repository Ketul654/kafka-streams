package com.ketul.kafka.stream;

import com.ketul.kafka.utils.StreamConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;



/**
 * Refer : https://www.confluent.io/blog/test-kafka-streams-with-topologytestdriver/
 */
public class WordCountStreamApplicationTest {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String,String> inputTopic;
    private TestOutputTopic<String,Long> outputTopic;
    private Serde<String> stringSerde = new Serdes.StringSerde();
    private Serde<Long> longSerde = new Serdes.LongSerde();

    @Before
    public void setUp() throws Exception {

        WordCountStreamApplication application = new WordCountStreamApplication();
        Method method = WordCountStreamApplication.class.getDeclaredMethod("createTopology");
        method.setAccessible(true);
        Topology topology = (Topology) method.invoke(application, null);
        method.setAccessible(false);

        method = WordCountStreamApplication.class.getDeclaredMethod("getStreamProperties");
        method.setAccessible(true);
        Properties properties = (Properties) method.invoke(application, null);
        method.setAccessible(false);

        topologyTestDriver = new TopologyTestDriver(topology, properties);

        inputTopic = topologyTestDriver.createInputTopic(StreamConstants.WORD_INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
        outputTopic = topologyTestDriver.createOutputTopic(StreamConstants.WORD_OUTPUT_TOPIC, stringSerde.deserializer(), longSerde.deserializer());
    }

    @Test
    public void testWordCount(){

        inputTopic.pipeInput("kafka stream application");
        inputTopic.pipeInput("Kafka Connect Application");
        inputTopic.pipeInput("KAFKA STREAM APPLICATION");
        Map<String, Long> wordCountMap = outputTopic.readKeyValuesToMap();
        Assert.assertEquals(wordCountMap.get("kafka").longValue(), 3L);
        Assert.assertEquals(wordCountMap.get("stream").longValue(), 2L);
        Assert.assertEquals(wordCountMap.get("application").longValue(), 3L);
        Assert.assertEquals(wordCountMap.get("connect").longValue(), 1L);
    }

    @After
    public void tearDown() throws Exception {
        topologyTestDriver.close();
    }
}