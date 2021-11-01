package com.BCI.xirr;


import java.util.ArrayList;
import java.time.LocalDate;

public class calc_xirr_fn {

    public final static double calc_xirr() {

        ArrayList<Transaction> txs = new ArrayList();
        double txnAmount;
        CharSequence txnWhen;
        // LocalDate txnWhen;


        // String [] stAmount = arrAmount.split(",",0);
        // String [] stWhen   = arrWhen.split(",",0);

        // for (int i = 0; i < stAmount.length; i++) {
            
        //     txnAmount = Double.parseDouble(stAmount[i]);
        //     txnWhen = stWhen[i];

        //     System.out.println("when:" + txnWhen + "  amount:" + txnAmount);
        //     txs.add(new Transaction(txnAmount, stWhen[i]));

        // }

        txs.add(new Transaction(-1000, "2015-01-15"));
        txs.add(new Transaction(-2500, "2015-02-08"));
        txs.add(new Transaction(-1000, "2015-04-17"));
        txs.add(new Transaction(5050, "2015-08-24"));
        txs.add(new Transaction(-2200.15, "2016-02-08"));
        txs.add(new Transaction(-1013.27, "2017-04-17"));
        txs.add(new Transaction(3050, "2019-08-24"));
        txs.add(new Transaction(-1000, "2016-01-15"));
        txs.add(new Transaction(-2500, "2017-02-08"));
        txs.add(new Transaction(-1000, "2016-04-17"));
        txs.add(new Transaction(5050, "2016-08-24"));
        txs.add(new Transaction(-2200.15, "2017-02-08"));
        txs.add(new Transaction(-1013.27, "2018-04-17"));
        txs.add(new Transaction(3050, "2020-08-24"));

        double rate = new XIRR(txs).xirr();
        return rate;
    }
 
}