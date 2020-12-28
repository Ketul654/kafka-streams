package com.ketul.kafka.stream;

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
import java.util.Map;
import java.util.Properties;


public class FavouriteColourStreamApplicationTest {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String,String> inputTopic;
    private TestOutputTopic<String,Long> outputTopic;
    private Serde<String> stringSerde = new Serdes.StringSerde();
    private Serde<Long> longSerde = new Serdes.LongSerde();

    @Before
    public void setUp() throws Exception {
        FavouriteColourStreamApplication application = new FavouriteColourStreamApplication();
        Method method = FavouriteColourStreamApplication.class.getDeclaredMethod("createTopology");
        method.setAccessible(true);
        Topology topology = (Topology) method.invoke(application, null);
        method.setAccessible(false);

        method = FavouriteColourStreamApplication.class.getDeclaredMethod("getStreamProperties");
        method.setAccessible(true);
        Properties properties = (Properties) method.invoke(application, null);
        method.setAccessible(false);

        topologyTestDriver = new TopologyTestDriver(topology, properties);

        inputTopic = topologyTestDriver.createInputTopic(StreamConstants.FAVOURITE_COLOUR_INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
        outputTopic = topologyTestDriver.createOutputTopic(StreamConstants.FAVOURITE_COLOUR_OUTPUT_TOPIC, stringSerde.deserializer(), longSerde.deserializer());
    }

    @Test
    public void testWithAllValidValues(){
        pipeAllValidInputs();
        Map<String, Long> favColourMap = outputTopic.readKeyValuesToMap();
        Assert.assertEquals(favColourMap.size(), 3);
        Assert.assertEquals(favColourMap.get("red").longValue(), 6L);
        Assert.assertEquals(favColourMap.get("green").longValue(), 2L);
        Assert.assertEquals(favColourMap.get("blue").longValue(), 2L);
    }

    @Test
    public void testWithUpdateColour(){
        pipeAllValidInputs();
        inputTopic.pipeInput("dipak", "green");
        inputTopic.pipeInput("bhumi", "green");
        Map<String, Long> favColourMap = outputTopic.readKeyValuesToMap();
        Assert.assertEquals(favColourMap.get("red").longValue(), 6L);
        Assert.assertEquals(favColourMap.get("green").longValue(), 4L);
        Assert.assertEquals(favColourMap.get("blue").longValue(), 0L);
    }

    @Test
    public void testWithInvalidColour(){
        pipeAllValidInputs();
        Map<String, Long> favColourMap = outputTopic.readKeyValuesToMap();
        Assert.assertEquals(favColourMap.size(), 3);
        Assert.assertEquals(favColourMap.get("red").longValue(), 6L);
        Assert.assertEquals(favColourMap.get("green").longValue(), 2L);
        Assert.assertEquals(favColourMap.get("blue").longValue(), 2L);

        inputTopic.pipeInput("ketul", "yellow");
        favColourMap = outputTopic.readKeyValuesToMap();
        Assert.assertEquals(favColourMap.size(), 1);
        Assert.assertEquals(favColourMap.get("red").longValue(), 5L);

        inputTopic.pipeInput("bhumi", "orange");
        inputTopic.pipeInput("vipul", "yellow");
        favColourMap = outputTopic.readKeyValuesToMap();
        Assert.assertEquals(favColourMap.size(), 2);
        Assert.assertEquals(favColourMap.get("blue").longValue(), 1L);
        Assert.assertEquals(favColourMap.get("green").longValue(), 1L);
    }

    private void pipeAllValidInputs() {
        inputTopic.pipeInput("ketul", "red");
        inputTopic.pipeInput("bhumi", "blue");
        inputTopic.pipeInput("vipul", "green");
        inputTopic.pipeInput("rony", "red");
        inputTopic.pipeInput("alpesh", "red");
        inputTopic.pipeInput("piyush", "yellow");
        inputTopic.pipeInput("umesh", "red");
        inputTopic.pipeInput("vinod", "green");
        inputTopic.pipeInput("dipak", "blue");
        inputTopic.pipeInput("praful", "yellow");
        inputTopic.pipeInput("bilu", "red");
        inputTopic.pipeInput("nitin", "red");
    }

    @After
    public void tearDown() throws Exception {
        topologyTestDriver.close();
    }
}