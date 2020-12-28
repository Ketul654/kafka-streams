package com.ketul.kafka.message;

import java.time.Instant;

public class BankTransaction {
    private String name;
    private float amount;
    private Instant time;

    public BankTransaction(){}

    public BankTransaction(String name, float amount, Instant time) {
        this.name = name;
        this.amount = amount;
        this.time = time;
    }

    public String getName() {
        return name;
    }

    public float getAmount() {
        return amount;
    }

    public Instant getTime() {
        return time;
    }

    @Override
    public String toString() {
        return "BankTransaction{" +
                "name='" + name + '\'' +
                ", amount=" + amount +
                ", time=" + time +
                '}';
    }
}
