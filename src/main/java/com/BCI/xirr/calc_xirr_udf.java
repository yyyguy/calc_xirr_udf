package com.BCI.xirr;

import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;

import javax.inject.Inject;

// Import dependencies
import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;

import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

// Function Template configuration
@SuppressWarnings("rawtypes")
@FunctionTemplate(
        name = "calc_xirr_udf",
        scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)

// CalcXIRR implements the AggregateFunction because
// there are multiple records combined to perform the XIRR calculation
public class calc_xirr_udf implements AggrFunction {

    @Param NullableVarCharHolder in_when;
    @Param NullableFloat8Holder in_amount;
    @Output NullableFloat8Holder out_rate;

    @Workspace XIRR.Builder xirr;
    @Workspace ArrayList txs;
    @Workspace NullableIntHolder init;
    @Workspace BigIntHolder nonNullCount;
    @Inject FunctionErrorContext errCtx;

    // The setup() function is used to initialize the workspace variables.
    public void setup() {

        System.out.println("STDOUT: Calling setup() in calc_xirr_udf ");

        // Initialize the working variables
        nonNullCount = new BigIntHolder();
        nonNullCount.value = 0;
        in_when = new NullableVarCharHolder();
        in_amount = new NullableFloat8Holder();
        init = new NullableIntHolder();
        init.value = 0;

        txs = new ArrayList();

        xirr = new XIRR.Builder();
        out_rate = new NullableFloat8Holder();
        out_rate.value = 0;
    }

    // The add() function applies consistent logic against each record within the dataset.
    @Override
    public void add() {
        sout: {

            System.out.println("STDOUT: Calling add() in CalcXIRR");

            // Check to see if the record's amount is available
            if (in_amount.isSet != 0) {
                nonNullCount.value = 1;
    
                // Add the record's amount and date to the transactions array
                boolean add = txs.add(new Transaction(in_amount.value, in_when.buffer.toString()));
                if (add = true) {
                    System.out.println("STDOUT: Successfully added transaction to the array.");
                }
                else {
                    System.err.println("STDERR: Error in adding transaction to the array.");
                }
    
            }

        } // end of sout block

    }

    // The output() function produces the return result which in this case is the internal rate of return.
    @Override
    public void output() {

        System.out.println("STDOUT: Calling output() in CalcXIRR");

        // // Call the XIRR calculation function with the loaded transactions array as the input argument
        out_rate.value = new XIRR(unmodifiableList(txs)).xirr();
        System.out.println(out_rate.value);
    }

    // The reset() function applies the necessary reset values to the required variables.
    @Override
    public void reset() {

        System.out.println("STDOUT: Calling reset() in calc_xirr_udf ");

        nonNullCount.value = 0; // Reset the null check
        out_rate.value         = 0; // Reset the rate.value
    }
}
