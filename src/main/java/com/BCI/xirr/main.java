package com.BCI.xirr;

// import java.util.ArrayList;

public class main {
    public static void main(String[] args) {
        System.out.println("Anyone for an XIRR Calculation?");

        String arrWhen = "2015-01-15,2015-02-08,2015-04-17,2015-08-24,2016-02-08,2017-04-17,2019-08-24,2019-10-11,2020-01-11";
        String arrAmount = "-4000000,-2500000,-1000000,5050000,-2200000.15,-1013000.27,3050000,4200123,1200234";
        String rate = com.BCI.xirr.calc_xirr_fn.calc_xirr(arrAmount, arrWhen);

        System.out.println(rate);
    }
}
