package com.BCI.xirr;

import java.util.ArrayList;
import java.util.StringTokenizer;

public class calc_xirr_fn {

    public final static double calc_xirr(String arrAmount, String arrWhen) {

        ArrayList<Transaction> txs = new ArrayList();

        System.out.println(arrWhen);
        System.out.println(arrAmount);

        StringTokenizer stWhen   = new StringTokenizer(arrWhen,",");
        StringTokenizer stAmount = new StringTokenizer(arrAmount,",");

        while (stWhen.hasMoreElements() && stAmount.hasMoreElements()) {
            System.out.println("when:" + stWhen.nextElement().toString() + "  amount:" + Double.parseDouble(stAmount.nextElement().toString()));

            //txs.add(new Transaction(Double.parseDouble(stAmount.nextElement().toString()), stWhen.nextToken()));
        }
  
        //double rate = new XIRR(txs).xirr();
        //System.out.println(rate);
        return 0;

    }

}