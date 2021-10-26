package com.BCI.xirr;

// Import dependencies
import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.*;
import java.util.ArrayList;
import static java.util.Collections.unmodifiableList;

// Function Template configuration
@SuppressWarnings("rawtypes")
@FunctionTemplate(
        name = "calc_xirr_udf",
        scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)

// CalcXIRR implements the AggregateFunction because
// there are multiple records combined to perform the XIRR calculation
public class CalcXIRR implements AggrFunction {

    @Param
    private NullableVarCharHolder when;
    private NullableFloat8Holder amount;

    @Output
    private Float8Holder rate;

    @Workspace
    XIRR.Builder xirr;
    ArrayList txs;
    NullableIntHolder init;
    BigIntHolder nonNullCount;

    // The setup() function is used to initialize the workspace variables.
    public void setup() {

        System.out.println("STDOUT: Calling setup() in calc_xirr_udf ");
        System.err.println("STDERR: Calling setup() in calc_xirr_udf ");

        // Initialize the working variables
        nonNullCount = new BigIntHolder();
        nonNullCount.value = 0;
        when = new NullableVarCharHolder();
        amount = new NullableFloat8Holder();
        init = new NullableIntHolder();
        init.value = 0;

        txs = new ArrayList();

        xirr = new XIRR.Builder();
        rate = new Float8Holder();
        rate.value = 0;
    }

    // The add() function applies consistent logic against each record within the dataset.
    @Override
    public void add() {

        System.out.println("STDOUT: Calling add() in CalcXIRR");

        // Check to see if the record's amount is available
        if (amount.isSet != 0) {
            nonNullCount.value = 1;

            // Add the record's amount and date to the transactions array
            boolean add = txs.add(new Transaction(amount.value, when.buffer.toString()));

        }
    }

    // The output() function produces the return result which in this case is the internal rate of return.
    @Override
    public void output() {

        System.out.println("STDOUT: Calling output() in CalcXIRR");

        // Call the XIRR calculation function with the loaded transactions array as the input argument
        rate.value = new XIRR(unmodifiableList(txs)).xirr();
        System.out.println(rate.value);
    }

    // The reset() function applies the necessary reset values to the required variables.
    @Override
    public void reset() {

        System.out.println("STDOUT: Calling reset() in calc_xirr_udf ");

        nonNullCount.value = 0; // Reset the null check
        rate.value         = 0; // Reset the rate.value
    }
}
