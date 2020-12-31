package com.ketul.kafka.message;

import java.time.Instant;

public class AccountDetails {
    private String customerId;
    private long accountNumber;
    private float lastBankBalance;
    private Instant accountCreationTime;
    private Instant accountLastUsed;

    public AccountDetails(){}

    public AccountDetails(String customerId, long accountNumber, float lastBankBalance, Instant accountCreationTime, Instant accountLastUsed) {
        this.customerId = customerId;
        this.accountNumber = accountNumber;
        this.lastBankBalance = lastBankBalance;
        this.accountCreationTime = accountCreationTime;
        this.accountLastUsed = accountLastUsed;
    }

    public String getCustomerId() {
        return customerId;
    }

    public long getAccountNumber() {
        return accountNumber;
    }

    public float getLastBankBalance() {
        return lastBankBalance;
    }

    public Instant getAccountCreationTime() {
        return accountCreationTime;
    }

    public Instant getAccountLastUsed() {
        return accountLastUsed;
    }

    @Override
    public String toString() {
        return "AccountDetails{" +
                "customerId='" + customerId + '\'' +
                ", accountNumber=" + accountNumber +
                ", lastBankBalance=" + lastBankBalance +
                ", accountCreationTime=" + accountCreationTime +
                ", accountLastUsed=" + accountLastUsed +
                '}';
    }
}
