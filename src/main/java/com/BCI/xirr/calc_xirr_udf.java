package com.BCI.xirr;

// Import dependencies
import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;

import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import org.apache.arrow.memory.ArrowBuf;
import javax.inject.Inject;

// Function Template configuration
@SuppressWarnings("rawtypes")
@FunctionTemplate(
        name = "calc_xirr_udf",
        scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)

// CalcXIRR implements the AggregateFunction because
// there are multiple records combined to perform the XIRR calculation
public class calc_xirr_udf implements AggrFunction {

    @Param NullableVarCharHolder     whenInHolder;
    @Param NullableVarCharHolder     amountInHolder;
    @Param NullableVarCharHolder     separatorInHolder;
    @Output NullableFloat8Holder     rateOutHolder;

    @Workspace NullableIntHolder     init;
    @Workspace NullableIntHolder     nonNullCount;
    @Workspace VarCharHolder         arrWhen;
    @Workspace VarCharHolder         arrAmount;
    @Workspace NullableVarCharHolder amountConvHolder;

    @Inject ArrowBuf whenBuffer;
    @Inject ArrowBuf amountBuffer;

    // The setup() function is used to initialize the workspace variables.
    public void setup() {
        System.out.println("STDOUT: Calling setup() in calc_xirr_udf ");
        new com.BCI.xirr.calcXIRR();

        // Initialize the working variables
        nonNullCount = new NullableIntHolder();
        nonNullCount.value = 0;
        init = new NullableIntHolder();
        init.value = 0;
    }

    // The add() function applies consistent logic against each record within the dataset.
    @Override
    public void add() {
            System.out.println("STDOUT: Calling add() in CalcXIRR");

            // txs.add(new Transaction(amount, when));

            // Determine the number of bytes for each input parameter
            final int bytesWhen      = whenInHolder.end - whenInHolder.start;
            final int bytesAmount    = amountInHolder.end - amountInHolder.start;
            final int bytesSeparator = separatorInHolder.end - separatorInHolder.start;

            // Check to see if the record's amount is available
            if (amountInHolder.isSet != 0) {
                nonNullCount.value = 1;

                // Reallocate enough memory to add the input values into each respective buffer;
                arrAmount.buffer = amountBuffer    = amountBuffer.reallocIfNeeded(bytesAmount);
                arrAmount.buffer = amountBuffer    = amountBuffer.reallocIfNeeded(bytesSeparator);
                  arrWhen.buffer = whenBuffer      = whenBuffer.reallocIfNeeded(bytesWhen);
                  arrWhen.buffer = whenBuffer      = whenBuffer.reallocIfNeeded(bytesSeparator);

                arrAmount.buffer.getBytes(arrAmount.start, amountBuffer, 0, bytesAmount + bytesSeparator);
                arrWhen.buffer.getBytes(arrWhen.start, whenBuffer, 0, bytesWhen + bytesSeparator); 
            }
    }

    // The output() function produces the return result which in this case is the internal rate of return.
    @Override
    public void output() {

        System.out.println("STDOUT: Calling output() in CalcXIRR");

        // Call the XIRR calculation function with the loaded transactions array as the input argument
        rateOutHolder.value = 
    }

    // The reset() function applies the necessary reset values to the required variables.
    @Override
    public void reset() {

        System.out.println("STDOUT: Calling reset() in calc_xirr_udf ");

        nonNullCount.value = 0; // Reset the null check
        // rateOutHolder.value     = 0; // Reset the rate.value
    }
}
