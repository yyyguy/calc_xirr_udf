package com.BCI.xirr;

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
    @Param NullableVarCharHolder  separatorInHolder;
    @Output NullableVarCharHolder rateOutHolderString;

    @Workspace NullableIntHolder     init;
    @Workspace NullableIntHolder     nonNullCount;
    @Workspace NullableVarCharHolder arrWhen;
    @Workspace NullableVarCharHolder arrAmount;
    @Workspace NullableVarCharHolder rateOutHolderTemp;
    @Workspace NullableVarCharHolder amountConvHolder;

    @Inject ArrowBuf whenBuffer;
    @Inject ArrowBuf amountBuffer;
    @Inject ArrowBuf tempBuffer;

    // The setup() function is used to initialize the workspace variables.
    public void setup() {

     }

    // The add() function applies consistent logic against each record within the dataset.
    @Override
    public void add() {
       System.out.println("STDOUT: Calling add() in CalcXIRR");

       // Local variables
       final byte[] bytesWhen;
       int whenLength;
       final byte[] bytesAmount;
       int amountLength;
       final byte[] bytesSeparator;
       int separatorLength;
       String txnWhen;
       String txnAmount;

       if (whenInHolder.isSet != 0 && amountInHolder.isSet != 0) {
            nonNullCount.value = 1;

            if (init.value == 0) {

                init.value = 1;
                nonNullCount.value = 1;
                
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
          else {
                // Determine the number of bytes for each input parameter
                txnWhen = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(whenInHolder.start, whenInHolder.end, whenInHolder.buffer);
                bytesWhen      = txnWhen.getBytes();
                whenLength     = bytesWhen.length;
                arrWhen.buffer = whenBuffer      = whenBuffer.reallocIfNeeded(whenLength);
                arrWhen.buffer = whenBuffer      = whenBuffer.reallocIfNeeded(bytesSeparator.length);

                txnAmount = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(amountInHolder.start, amountInHolder.end, amountInHolder.buffer);
                bytesAmount = txnAmount.getBytes();
                amountLength = bytesAmount.length;
                arrAmount.buffer = amountBuffer = amountBuffer.reallocIfNeeded((long)amountLength);
                arrAmount.start = 0;
                arrAmount.end = amountLength;
                arrAmount.buffer.setBytes(0, bytesAmount, 0, amountLength);

                //bytesSeparator = separatorInHolder.end - separatorInHolder.start;

            }
 
       }
    }

    // The output() function produces the return result which in this case is the internal rate of return.
    @Override
    public void output() {

        System.out.println("STDOUT: Calling output() in CalcXIRR");

	    rateOutHolderTemp = new NullableVarCharHolder();
	    rateOutHolderTemp.isSet = 1;

        // Call the XIRR calculation function with the loaded transactions array as the input argument
        // rateOutHolder.value = com.dremio.example_udfs.calc_xirr_fn.calc_xirr("-1000,-2000,5050", "2015-01-12, 2016-02-14, 2017-04-16");
	String tmp = com.BCI.xirr.calc_xirr_fn.calc_xirr("-1000,-2000,5050", "2015-01-12, 2016-02-14, 2017-04-16");
	final byte[] b = tmp.getBytes();
	int finallength = b.length;
	rateOutHolderTemp.buffer = tempBuffer = tempBuffer.reallocIfNeeded((long)finallength);
	rateOutHolderTemp.start = 0;
	rateOutHolderTemp.end = finallength;
	rateOutHolderTemp.buffer.setBytes(0, b, 0, finallength);
	rateOutHolderString = rateOutHolderTemp;
    }

    // The reset() function applies the necessary reset values to the required variables.
    @Override
    public void reset() {

        System.out.println("STDOUT: Calling reset() in calc_xirr_udf ");

        nonNullCount.value = 0; // Reset the null check

    }
}
