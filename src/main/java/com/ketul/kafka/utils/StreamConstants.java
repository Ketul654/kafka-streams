package com.ketul.kafka.utils;

public class StreamConstants {

    public static final String APPLICATION_ID = "customer-stream-application";
    public static final String INPUT_TOPIC = "customer-input";
    public static final String OUTPUT_TOPIC = "customer-output";
    public static final String OUTPUT_COMPACTED_TOPIC = "customer-compacted-output";
    public static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";

    private StreamConstants(){}
}
