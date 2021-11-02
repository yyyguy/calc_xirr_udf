package com.BCI.xirr;

import java.util.ArrayList;

//import java.time.LocalDate;
@SuppressWarnings("unchecked")

public class calc_xirr_fn {

    public final static String calc_xirr(String arrAmount, String arrWhen) {

        ArrayList<Transaction> txs = new ArrayList();

        String [] stAmount = arrAmount.split(",",0);
        String [] stWhen   = arrWhen.split(",",0);
        Double txnAmount;
        String txnWhen = new String();
        StringBuffer sbf1 = new StringBuffer();

        for (int i = 0; i < stAmount.length; i++) {
            
             txnAmount = Double.parseDouble(stAmount[i]);
             txnWhen = stWhen[i];

             System.out.println("when:" + txnWhen + " amount:" + txnAmount);
             txs.add(new Transaction(txnAmount, txnWhen));

             System.out.println(sbf1);
     
             sbf1.append(stWhen[i]);
             sbf1.append(",");
     
             System.out.println(sbf1);

        }

        double rate = new XIRR(txs).xirr();
        return Double.toString(rate);
    }
 
}