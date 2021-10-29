package com.BCI.xirr;

import java.util.ArrayList;

public class calcXIRR {
    double amountIn;
    String whenIn;
    ArrayList<Transaction> txs = new ArrayList();

    public void reset() {
        amountIn = 0;
        whenIn = null;
    }

    public void add(double inAmount, String inWhen) {
        txs.add(new Transaction(inAmount, inWhen));
    }

    public double output() {

        // Parse each array

        // Convert to numeric , etc.,

        // Load the values into the Transaction();
        // Load each Transaction into an Array (txs)

        txs.add(new Transaction(-1000, "2015-01-15"));

        double rateOut = new com.BCI.xirr.XIRR(txs).xirr();
        return rateOut;
    }
}