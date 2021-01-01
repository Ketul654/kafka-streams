package com.ketul.kafka.message;

public class TransactionStatus {
    private float previousBalance;
    private float amountWithdrawn;
    private float amountDeposited;

    public TransactionStatus(){}

    public TransactionStatus(float previousBalance, float amountWithdrawn, float amountDeposited) {
        this.previousBalance = previousBalance;
        this.amountWithdrawn = amountWithdrawn;
        this.amountDeposited = amountDeposited;
    }

    public float getPreviousBalance() {
        return previousBalance;
    }

    public void setPreviousBalance(float previousBalance) {
        this.previousBalance = previousBalance;
    }

    public float getAmountWithdrawn() {
        return amountWithdrawn;
    }

    public void setAmountWithdrawn(float amountWithdrawn) {
        this.amountWithdrawn = amountWithdrawn;
    }

    public float getAmountDeposited() {
        return amountDeposited;
    }

    public void setAmountDeposited(float amountDeposited) {
        this.amountDeposited = amountDeposited;
    }

    @Override
    public String toString() {
        return "TransactionStatus{" +
                "previousBalance=" + previousBalance +
                ", amountWithdrawn=" + amountWithdrawn +
                ", amountDeposited=" + amountDeposited +
                '}';
    }
}
