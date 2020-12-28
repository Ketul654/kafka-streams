package com.ketul.kafka.utils;

public class StreamConstants {

    public static final String WORD_COUNT_APPLICATION_ID = "word-count-stream-application";
    public static final String FAVOURITE_COLOUR_APPLICATION_ID = "favourite-colour-stream-application";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";
    public static final String WORD_INPUT_TOPIC = "word-count-input";
    public static final String WORD_OUTPUT_TOPIC = "word-count-output";
    public static final String FAVOURITE_COLOUR_INPUT_TOPIC = "favourite-colour-input";
    public static final String FAVOURITE_COLOUR_OUTPUT_TOPIC = "favourite-colour-output";
    public static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";
    public static final String BANK_TRANSACTIONS_TOPIC = "bank-transactions";
    public static final String BANK_BALANCES_TOPIC = "bank-balances";
    public static final String ALL_ACKS = "all";
    public static final String RETRIES = "3";
    public static final boolean ENABLE_IDEMPOTENCE = true;
    public static final int LINGER_MS = 1;

    private StreamConstants(){}
}
