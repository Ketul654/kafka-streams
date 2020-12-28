package com.ketul.kafka.message;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;

public class BankTransactionTest {
    @Test
    public void testGetters(){
        BankTransaction bankTransaction = new BankTransaction("ketul", 112.34f, Instant.parse("2020-10-01T10:30:01Z"));
        Assert.assertTrue(bankTransaction.getName().equals("ketul"));
        Assert.assertTrue(bankTransaction.getAmount() == 112.34f);
        Assert.assertTrue(bankTransaction.getTime().equals(Instant.parse("2020-10-01T10:30:01Z")));
    }

    @Test
    public void testToString(){
        BankTransaction bankTransaction = new BankTransaction("ketul", 112.34f, Instant.parse("2020-10-01T10:30:01Z"));
        Assert.assertTrue(bankTransaction.toString().contains("2020-10-01T10:30:01Z"));
    }
}