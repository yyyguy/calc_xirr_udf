package com.BCI.xirr;

/**
 * Indicates that the algorithm failed to converge due to one of the values 
 * (either the candidate value, the function value or derivative value) being
 * an invalid double (NaN, Infinity or -Infinity) or other condition leading to
 * an overflow.
 */
public class OverflowException extends ArithmeticException {

    private final NewtonRaphson.Calculation state;

    OverflowException(String message, NewtonRaphson.Calculation state) {
        super(message);
        this.state = state;
    }

// --Commented out by Inspection START (10/19/2021 8:27 a.m.):
//    /*
//     * Get the initial guess used by the algorithm.
//     * @return the initial guess
//     */
//    public double getInitialGuess() {
//        return state.getGuess();
//    }
// --Commented out by Inspection STOP (10/19/2021 8:27 a.m.)

// --Commented out by Inspection START (10/19/2021 8:27 a.m.):
//    /*
//     * Get the number of iterations passed when encountering the overflow.
//     * @return the number of iterations passed when encountering the overflow
//     * condition
//     */
//    public long getIteration() {
//        return state.getIteration();
//    }
// --Commented out by Inspection STOP (10/19/2021 8:27 a.m.)

// --Commented out by Inspection START (10/19/2021 8:27 a.m.):
//    /*
//     * Get the candidate value when the overflow condition occurred.
//     * @return the candidate value when the overflow condition occurred
//     */
//    public double getCandidate() {
//        return state.getCandidate();
//    }
// --Commented out by Inspection STOP (10/19/2021 8:27 a.m.)

// --Commented out by Inspection START (10/19/2021 8:27 a.m.):
//    /*
//     * Get the function value when the overflow condition occurred.
//     * @return the function value when the overflow condition occurred
//     */
//    public double getValue() {
//        return state.getValue();
//    }
// --Commented out by Inspection STOP (10/19/2021 8:27 a.m.)

// --Commented out by Inspection START (10/19/2021 8:27 a.m.):
//    /*
//     * Get the derivative value when the overflow condition occurred.  A null
//     * value indicates the derivative was not yet calculated.
//     * @return the derivative value when the overflow condition occurred
//     */
//    public Double getDerivativeValue() {
//        return state.getDerivativeValue();
//    }
// --Commented out by Inspection STOP (10/19/2021 8:27 a.m.)

    @Override
    public String toString() {
        return super.toString() + ": " + state;
    }

}
