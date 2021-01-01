package com.ketul.kafka.aggregator;

import com.ketul.kafka.message.AccountDetails;
import com.ketul.kafka.message.Customer;
import com.ketul.kafka.message.TransactionStatus;
import com.ketul.kafka.stream.transform.stateful.StreamAggregation;
import org.apache.kafka.streams.kstream.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionStatusAggregator implements Aggregator<String, Customer, TransactionStatus> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamAggregation.class);

    @Override
    public TransactionStatus apply(String customerId, Customer customer, TransactionStatus aggregatedTransactionStatus) {
        LOGGER.info("Aggregated transaction status : {} for {}", aggregatedTransactionStatus.toString(), customerId);
        LOGGER.info("New customer details : {} for {}", customer.toString(), customerId);
        AccountDetails newAccountDetails = customer.getAccountDetails();
        float amountWithdrawn = aggregatedTransactionStatus.getAmountWithdrawn();
        float amountDeposited = aggregatedTransactionStatus.getAmountDeposited();
        float newPreviousTransaction = customer.getAccountDetails().getLastBankBalance();
        if(newAccountDetails.getLastBankBalance() > aggregatedTransactionStatus.getPreviousBalance()) {
            amountDeposited += newAccountDetails.getLastBankBalance() - aggregatedTransactionStatus.getPreviousBalance();
        } else if(newAccountDetails.getLastBankBalance() < aggregatedTransactionStatus.getPreviousBalance()) {
            amountWithdrawn += aggregatedTransactionStatus.getPreviousBalance() - newAccountDetails.getLastBankBalance();
        }
        TransactionStatus newTransactionStatus = new TransactionStatus(newPreviousTransaction, amountWithdrawn, amountDeposited);
        LOGGER.info("New transaction status : {} for {}", newTransactionStatus.toString(), customerId);
        return newTransactionStatus;
    }
}
