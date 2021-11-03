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
//import com.google.common.base.Charsets.UTF_8;

// Function Template configuration
@FunctionTemplate(
        name = "calc_xirr_udf",
        scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)

// CalcXIRR implements the AggregateFunction because
// there are multiple records combined to perform the XIRR calculation
public class calc_xirr_udf implements AggrFunction {

    @Param NullableVarCharHolder  whenInHolder;
    @Param NullableVarCharHolder  amountInHolder;
    @Output NullableVarCharHolder rateOutHolder;

    @Workspace NullableIntHolder     init;
    @Workspace BigIntHolder          nonNullCount;
    @Workspace NullableVarCharHolder separator;
    @Workspace NullableVarCharHolder whenValues;
    @Workspace NullableVarCharHolder amountValues;

    @Inject ArrowBuf whenBuffer;
    @Inject ArrowBuf amountBuffer;
    @Inject ArrowBuf tempBuffer;
    @Inject ArrowBuf rateBuffer;

    // The setup() function is used to initialize the workspace variables.
    public void setup() {

        init = new NullableIntHolder();
        init.value = 0;
        nonNullCount = new BigIntHolder();
        nonNullCount.value = 0;

        whenValues = new NullableVarCharHolder();
        whenValues.isSet = 0;
        amountValues = new NullableVarCharHolder();
        amountValues.isSet = 0;
        
     }

    // The add() function applies consistent logic against each record within the dataset.
    @Override
    public void add() {
       System.out.println("STDOUT: Calling add() in CalcXIRR");

        // Local variables
        String txnWhen = new String();
        String txnAmount = new String();
        String addWhen;
        String addAmount;
       
        // Access date text contained in the VarCharHolder called whenInHolder
        txnWhen = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(whenInHolder.start, whenInHolder.end, whenInHolder.buffer);

        // Append the current Amount value to the AmountValues
        addWhen = com.BCI.xirr.calc_xirr_fn.addArray(txnWhen, whenValues.buffer.toString());

        // Assign the date text to the whenValues VarCharHolder
        byte bytesWhen[] = addWhen.getBytes(java.nio.charset.Charset.forName("UTF-8"));
        whenBuffer = whenBuffer.reallocIfNeeded(bytesWhen.length);
        whenBuffer.setBytes(0, bytesWhen);
        whenValues.buffer = whenBuffer;
        whenValues.start = 0;
        whenValues.end = bytesWhen.length;

        // Access amount text contained in the VarCharHolder called whenInHolder
        txnAmount = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(amountInHolder.start, amountInHolder.end, amountInHolder.buffer);

        // Append the current Amount value to the AmountValues
        addAmount = com.BCI.xirr.calc_xirr_fn.addArray(txnAmount, amountValues.buffer.toString());

        // Assign the amount text to the amountValues VarCharHolder
        byte bytesAmount[] = addAmount.getBytes(java.nio.charset.Charset.forName("UTF-8"));
        amountBuffer = amountBuffer.reallocIfNeeded(bytesAmount.length);
        amountBuffer.setBytes(0, bytesAmount);
        amountValues.buffer = amountBuffer;
        amountValues.start = 0;
        amountValues.end = bytesAmount.length;

    }

    // The output() function produces the return result which in this case is the internal rate of return.
    @Override
    public void output() {

        System.out.println("STDOUT: Calling output() in CalcXIRR");

        // Call the XIRR calculation function with the loaded transactions array as the input argument
        // rateOutHolder.value = com.dremio.example_udfs.calc_xirr_fn.calc_xirr("-1000,-2000,5050", "2015-01-12, 2016-02-14, 2017-04-16");
        // String tmp = com.BCI.xirr.calc_xirr_fn.calc_xirr(amountValues, whenValues);

        String rate = com.BCI.xirr.calc_xirr_fn.calc_xirr(amountValues.buffer.toString(), whenValues.buffer.toString());
        
        byte bytesRate[] = rate.getBytes(java.nio.charset.Charset.forName("UTF-8"));
        rateBuffer = rateBuffer.reallocIfNeeded(bytesRate.length);
        rateBuffer.setBytes(0, bytesRate);
        rateOutHolder.buffer = rateBuffer;
        rateOutHolder.start = 0;
        rateOutHolder.end = bytesRate.length;

    }

    // The reset() function applies the necessary reset values to the required variables.
    @Override
    public void reset() {

        System.out.println("STDOUT: Calling reset() in calc_xirr_udf ");

        init.value = 0;
        nonNullCount.value = 0; // Reset the null check

    }
}
