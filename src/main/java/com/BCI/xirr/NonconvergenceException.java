
package com.BCI.xirr;

/**
 * Indicates the algorithm failed to converge in the allotted number of
 * iterations.
 * @author FredBlue
 */
public class NonconvergenceException extends IllegalArgumentException {

    private final double initialGuess;
    private final long iterations;

    public NonconvergenceException(double guess, long iterations) {
        super("Newton-Raphson failed to converge within " + iterations
                + " iterations.");
        this.initialGuess = guess;
        this.iterations = iterations;
    }

public long getIterations() {
        return iterations;
    }

}
