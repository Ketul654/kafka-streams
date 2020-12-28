package com.ketul.kafka.data;

import java.time.Instant;

public class BankBalance {
    private float balance;
    private Instant time;
    private int noOfTransaction;

    public BankBalance(){}

    public BankBalance(float balance, Instant time, int noOfTransaction){
        this.balance = balance;
        this.time = time;
        this.noOfTransaction = noOfTransaction;
    }

    public float getBalance() {
        return balance;
    }

    public Instant getTime() {
        return time;
    }

    public int getNoOfTransaction() {
        return noOfTransaction;
    }

    @Override
    public String toString() {
        return "BankBalance{" +
                "balance=" + balance +
                ", time=" + time +
                ", noOfTransaction=" + noOfTransaction +
                '}';
    }
}
