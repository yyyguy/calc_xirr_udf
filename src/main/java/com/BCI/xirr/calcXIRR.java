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

        double rateOut = new com.BCI.xirr.XIRR(txs).xirr();
        return rateOut;
    }
}