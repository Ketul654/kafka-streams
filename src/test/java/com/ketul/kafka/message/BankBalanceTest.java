package com.ketul.kafka.message;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;


public class BankBalanceTest {
    @Test
    public void testGetters(){
        BankBalance bankBalance = new BankBalance(1564.23f,  Instant.parse("2020-10-01T10:30:01Z"), 24);
        Assert.assertTrue(bankBalance.getCurrentBalance() == 1564.23f);
        Assert.assertTrue(bankBalance.getTotalTransactions() == 24);
        Assert.assertTrue(bankBalance.getLastTransactionTime().equals(Instant.parse("2020-10-01T10:30:01Z")));
    }

    @Test
    public void testToString(){
        BankBalance bankBalance = new BankBalance(1564.23f,  Instant.parse("2020-10-01T10:30:01Z"), 24);
        Assert.assertTrue(bankBalance.toString().contains("2020-10-01T10:30:01Z"));
    }
}