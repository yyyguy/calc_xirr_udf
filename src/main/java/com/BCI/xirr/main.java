package com.BCI.xirr;

import java.util.ArrayList;
import java.util.Arrays;

import static java.util.Collections.unmodifiableList;

public class main {
    public static void main(String[] args) {
        System.out.println("Anyone for an XIRR Calculation?");

        var transactions = new ArrayList();

        transactions.addAll(
                Arrays.asList(
                        new Transaction(-1000, "2016-01-15"),
                        new Transaction(-2500, "2016-02-08"),
                        new Transaction(-1000, "2016-04-17"),
                        new Transaction(5050, "2016-08-24")
                ));

        System.out.println(transactions.size());

        double rate = new XIRR(unmodifiableList(transactions)).xirr();
        System.out.println(rate);
    }
}
