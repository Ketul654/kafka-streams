package com.ketul.kafka.utils;

public class StreamConstants {

    public static final String APPLICATION_ID = "customer-stream-application";
    public static final String CONSUMER_INPUT_TOPIC = "customer-input";
    public static final String ACCOUNT_DETAILS_INPUT_TOPIC = "account-details-input";
    public static final String CONSUMER_OUTPUT_TOPIC = "customer-output";
    public static final String CONSUMER_OUTPUT_COMPACTED_TOPIC = "customer-compacted-output";
    public static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";
    public static final String INTERMEDIATE_TOPIC = "customer-intermediate";
    public static final String K_OUTPUT_TOPIC = "k-output";
    public static final String V_OUTPUT_TOPIC = "v-output";
    public static final String B_OUTPUT_TOPIC = "b-output";
    public static final String P_OUTPUT_TOPIC = "p-output";
    public static final String U_OUTPUT_TOPIC = "u-output";
    public static final String OTHER_OUTPUT_TOPIC = "other-output";

    private StreamConstants(){}
}
