package com.BCI.xirr;

import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import org.apache.arrow.memory.ArrowBuf;
import javax.inject.Inject;

import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Workspace;

// Function Template configuration
@FunctionTemplate(
        name = "calc_xirr_udf",
        scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)

// CalcXIRR implements the AggregateFunction because
// there are multiple records combined to perform the XIRR calculation
public class calc_xirr_udf implements AggrFunction {

    @Param NullableVarCharHolder  whenInHolder;
    @Param NullableVarCharHolder  amountInHolder;
    @Output NullableVarCharHolder rateOutHolderString;

    @Workspace NullableIntHolder     init;
    @Workspace BigIntHolder          nonNullCount;
    @Workspace NullableVarCharHolder separator;
    @Workspace NullableVarCharHolder arrWhen;
    @Workspace NullableVarCharHolder arrAmount;
    @Workspace NullableVarCharHolder amountConvHolder;

    @Inject ArrowBuf whenBuffer;
    @Inject ArrowBuf amountBuffer;
    @Inject ArrowBuf tempBuffer;

    // The setup() function is used to initialize the workspace variables.
    public void setup() {

        init = new NullableIntHolder();
        init.value = 0;
        nonNullCount = new BigIntHolder();
        nonNullCount.value = 0;

        arrWhen = new NullableVarCharHolder();
        arrWhen.isSet = 0;
        arrAmount = new NullableVarCharHolder();
        arrWhen.isSet = 0;
        
     }

    // The add() function applies consistent logic against each record within the dataset.
    @Override
    public void add() {
       System.out.println("STDOUT: Calling add() in CalcXIRR");

        // Local variables
        //final byte[] bytesWhen;
        //int whenLength;
        //final byte[] bytesAmount;
        //int amountLength;
        //String txnWhen;
        //String txnAmount;
       
       if (amountInHolder.isSet != 0 && whenInHolder.isSet != 0) {
           nonNullCount.value = 1;

           if (init.value == 0) {
               init.value = 1;

               String txnWhen = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(whenInHolder.start, whenInHolder.end, whenInHolder.buffer);
               final byte[] bytesWhen = txnWhen.getBytes();
               int whenLength = bytesWhen.length;
               arrWhen.buffer = whenBuffer = whenBuffer.reallocIfNeeded((long)whenLength);
               arrWhen.start = 0;
               arrWhen.end = whenLength;
               arrWhen.buffer.setBytes(0, bytesWhen, 0, whenLength);

               String txnAmount = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(amountInHolder.start, amountInHolder.end, amountInHolder.buffer);
               txnAmount.concat(",");
               final byte[] bytesAmount = txnAmount.getBytes();
               int amountLength = bytesAmount.length;
               arrAmount.buffer = amountBuffer = amountBuffer.reallocIfNeeded((long)amountLength);
               arrAmount.start = 0;
               arrAmount.end = amountLength;
               arrAmount.buffer.setBytes(0, bytesAmount, 0, amountLength);

           } else {
               java.lang.String strAmount = new java.lang.String(txnAmount.getBytes());

           }


       }

       // Local variables
       final byte[] bytesWhen;
       int whenLength;
       final byte[] bytesAmount;
       int amountLength;
       String txnWhen;
       String txnAmount;

                txnWhen = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(whenInHolder.start, whenInHolder.end, whenInHolder.buffer);
                bytesWhen = txnWhen.getBytes();
                whenLength = bytesWhen.length;
                arrWhen.buffer = whenBuffer = whenBuffer.reallocIfNeeded((long)whenLength);
                arrWhen.start = 0;
                arrWhen.end = whenLength;
                arrWhen.buffer.setBytes(0, bytesWhen, 0, whenLength);

                txnAmount = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(amountInHolder.start, amountInHolder.end, amountInHolder.buffer);
                bytesAmount = txnAmount.getBytes();
                amountLength = bytesAmount.length;
                arrAmount.buffer = amountBuffer = amountBuffer.reallocIfNeeded((long)amountLength);
                arrAmount.start = 0;
                arrAmount.end = amountLength;
                arrAmount.buffer.setBytes(0, bytesAmount, 0, amountLength);

    }

    // The output() function produces the return result which in this case is the internal rate of return.
    @Override
    public void output() {

        System.out.println("STDOUT: Calling output() in CalcXIRR");

        // Call the XIRR calculation function with the loaded transactions array as the input argument
        // rateOutHolder.value = com.dremio.example_udfs.calc_xirr_fn.calc_xirr("-1000,-2000,5050", "2015-01-12, 2016-02-14, 2017-04-16");
        // String tmp = com.BCI.xirr.calc_xirr_fn.calc_xirr(arrAmount, arrWhen);

        String tmp = com.BCI.xirr.calc_xirr_fn.calc_xirr("-1000,-2000,5050", "2015-01-12,2016-02-14,2017-04-16");
	final byte[] b = tmp.getBytes();
	int finallength = b.length;
	//rateOutHolderTemp.buffer = tempBuffer = tempBuffer.reallocIfNeeded((long)finallength);
	//rateOutHolderTemp.start = 0;
	//rateOutHolderTemp.end = finallength;
	//rateOutHolderTemp.buffer.setBytes(0, b, 0, finallength);
	//rateOutHolderString = rateOutHolderTemp;
    }

    // The reset() function applies the necessary reset values to the required variables.
    @Override
    public void reset() {

        System.out.println("STDOUT: Calling reset() in calc_xirr_udf ");

        init.value = 0;
        nonNullCount.value = 0; // Reset the null check

    }
}
