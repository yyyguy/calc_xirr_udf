package com.BCI.xirr;

import java.util.ArrayList;
//import java.util.Arrays;
import java.util.StringTokenizer;
//import java.util.stream.Stream;

public class main {
    public static void main(String[] args) {
        System.out.println("Anyone for an XIRR Calculation?");

        String arrWhen = "2015-01-15,2015-02-08,2015-04-17,2015-08-24,2016-02-08";
        String arrAmount = "-4000,-2500,-1000,5050,-2200.15";

        ArrayList<Transaction> transactions = new ArrayList();
        ArrayList<Transaction> txs = new ArrayList();

        StringTokenizer stWhen   = new StringTokenizer(arrWhen,",");
        StringTokenizer stAmount = new StringTokenizer(arrAmount,",");

        com.BCI.xirr.calc_xirr_fn.calc_xirr(arrAmount, arrWhen);

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

        double rate01 = new XIRR(txs).xirr();
        System.out.println(rate01);

    }
}
