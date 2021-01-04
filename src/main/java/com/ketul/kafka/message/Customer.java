package com.ketul.kafka.message;

public class Customer {
    private String customerId;
    private String name;
    private int age;
    private AccountDetails accountDetails;

    public Customer(){}

    public Customer(final String customerId, final String name, int age, final AccountDetails accountDetails) {
        this.customerId = customerId;
        this.name = name;
        this.age = age;
        this.accountDetails = accountDetails;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public AccountDetails getAccountDetails() {
        return accountDetails;
    }


    public void setAge(int age) {
        this.age = age;
    }

    public void setAccountDetails(AccountDetails accountDetails) {
        this.accountDetails = accountDetails;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "customerId='" + customerId + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", accountDetails=" + accountDetails +
                '}';
    }
}
