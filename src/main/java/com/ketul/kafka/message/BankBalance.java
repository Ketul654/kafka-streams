package com.ketul.kafka.message;

import java.time.Instant;

public class BankBalance {
    private float currentBalance;
    private Instant lastTransactionTime;
    private int totalTransactions;

    public BankBalance(){}

    public BankBalance(float currentBalance, Instant lastTransactionTime, int totalTransactions){
        this.currentBalance = currentBalance;
        this.lastTransactionTime = lastTransactionTime;
        this.totalTransactions = totalTransactions;
    }

    public float getCurrentBalance() {
        return currentBalance;
    }

    public Instant getLastTransactionTime() {
        return lastTransactionTime;
    }

    public int getTotalTransactions() {
        return totalTransactions;
    }

    @Override
    public String toString() {
        return "BankBalance{" +
                "balance=" + currentBalance +
                ", time=" + lastTransactionTime +
                ", noOfTransaction=" + totalTransactions +
                '}';
    }
}
