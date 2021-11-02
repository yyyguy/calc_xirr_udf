package com.BCI.xirr;

// import java.util.ArrayList;

public class main {
    public static void main(String[] args) {
        System.out.println("Anyone for an XIRR Calculation?");

        String arrWhen = "2015-01-15,2015-02-08,2015-04-17,2015-08-24,2016-02-08";
        String arrAmount = "-4000,-2500,-1000,5050,-2200.15";

        double rate = com.BCI.xirr.calc_xirr_fn.calc_xirr(arrAmount, arrWhen);

        System.out.println(rate);
    }
}
