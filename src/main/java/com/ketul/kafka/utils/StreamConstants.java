package com.ketul.kafka.utils;

public class StreamConstants {

    public static final String WORD_COUNT_APPLICATION_ID = "word-count-stream-application";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";
    public static final String INPUT_TOPIC = "word-count-input";
    public static final String OUTPUT_TOPIC = "word-count-output";
    public static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";

    private StreamConstants(){}
}
