package com.BCI.xirr;

import java.util.function.DoubleUnaryOperator;

/**
 * Simple implementation of the Newton-Raphson method for finding roots or
 * inverses of a function.
 *
 * The function and its derivative must be supplied as instances of
 * DoubleUnaryOperator and the answers are computed as doubles.
 *
 * For examples of usage, see the source of the test class or the XIRR class.
 *
 * The iterations parameter is used as an upper bound on the number
 * of iterations to run the method for.
 *
 * The tolerance parameter is used to determine when the method
 * has been successful.  If the value of the function at the candidate input
 * is within the tolerance of the desired target value, the
 * method terminates.
 */
public class NewtonRaphson {
    /** Default tolerance. */
    public static final double TOLERANCE = 0.000_000_1;

    /**
     * Convenience method for getting an instance of a {@link Builder}.
     * @return new Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    private final DoubleUnaryOperator func;
    private final DoubleUnaryOperator derivative;
    private final double tolerance;
    private final long iterations;

    /**
     * Construct an instance of the NewtonRaphson method for masochists who
     * do not want to use {@link #builder()}.
     * @param func the function
     * @param derivative the derivative of the function
     * @param tolerance the tolerance
     * @param iterations maximum number of iterations
     */
    public NewtonRaphson(
        DoubleUnaryOperator func,
        DoubleUnaryOperator derivative,
        double tolerance,
        long iterations) {
        this.func = func;
        this.derivative = derivative;
        this.tolerance = tolerance;
        this.iterations = iterations;
    }

    /**
     * Equivalent to <code>inverse(0, guess)</code>.
     * <p>
     * Find a root of the function starting at the given guess.  Equivalent to
     * invoking <code>inverse(0, guess)</code>.  Finds the input value <i>x</i>
     * such that |<i>f</i>(<i>x</i>)| &lt; <i>tolerance</i>.
     * @param guess the value to start at
     * @return an input to the function which yields zero within the given
     *         tolerance
     * @see #inverse(double, double) 
     */
    public double findRoot(final double guess) {
        return inverse(0, guess);
    }

    /**
     * Find the input value to the function which yields the given
     * <code>target</code>, starting at the <code>guess</code>.  More precisely,
     * finds an input value <i>x</i>
     * such that |<i>f</i>(<i>x</i>) - <code>target</code>| &lt; <i>tolerance</i>
     * @param target the target value of the function
     * @param guess value to start the algorithm with
     * @return the inverse of the function at <code>target</code> within the
     * given tolerance
     * @throws ZeroValuedDerivativeException if the derivative is 0 while
     *                                       executing the Newton-Raphson method
     * @throws OverflowException when a value involved is infinite or NaN
     * @throws NonconvergenceException if the method fails to converge in the
     *                                 given number of iterations
     */
    public double inverse(final double target, final double guess) {
        return new Calculation().solve(guess, target);
    }

    /**
     * Builder for {@link NewtonRaphson} instances.
     */
    public static class Builder {

        private DoubleUnaryOperator func;
        private DoubleUnaryOperator derivative;
        private double tolerance = TOLERANCE;
        private long iterations = 10_000;

        public Builder() {
        }

        public Builder withFunction(DoubleUnaryOperator func) {
            this.func = func;
            return this;
        }

        public Builder withDerivative(DoubleUnaryOperator derivative) {
            this.derivative = derivative;
            return this;
        }

        public Builder withTolerance(double tolerance) {
            this.tolerance = tolerance;
            return this;
        }

        public Builder withIterations(long iterations) {
            this.iterations = iterations;
            return this;
        }

        public NewtonRaphson build() {
            return new NewtonRaphson(func, derivative, tolerance, iterations);
        }

        /*
         * Convenience method which builds the NewtonRaphson instance and
         * invokes {@link NewtonRaphson#findRoot(double)}.
         * @param guess see {@link NewtonRaphson#findRoot(double)}
         * @return see {@link NewtonRaphson#findRoot(double)}
         */
        public double findRoot(double guess) {
            return build().findRoot(guess);
        }
    }

    class Calculation {
        private double guess;
        private long i; // persistent loop counter
        private double candidate;
        private double value;
        private Double derivativeValue;

        public double getGuess() {
            return guess;
        }

        public void setGuess(double guess) {
            this.guess = guess;
        }

        public long getIteration() {
            return i + 1;
        }

        public double getCandidate() {
            return candidate;
        }

        public void setCandidate(double candidate) {
            this.candidate = candidate;
            if (!Double.isFinite(candidate)) {
                throw new OverflowException("Candidate overflow", this);
            }
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
            if (!Double.isFinite(value)) {
                throw new OverflowException("Function value overflow", this);
            }
        }

        public Double getDerivativeValue() {
            return derivativeValue;
        }

        public void setDerivativeValue(Double derivativeValue) {
            this.derivativeValue = derivativeValue;
            if (!Double.isFinite(derivativeValue)) {
                throw new OverflowException("Derivative value overflow", this);
            } else if (derivativeValue == 0.0) {
                throw new ZeroValuedDerivativeException(this);
            }
        }

        @Override
        public String toString() {
            return '{'
                + "guess=" + guess
                + ", iteration="+ i
                + ", candidate=" + candidate
                + ", value=" + value
                + ", derivative=" + derivativeValue + '}';
        }

        double solve(double guess, double target) {
            setGuess(guess);
            setCandidate(guess);
            for (i = 0; i < iterations; i++) {
                setValue(func.applyAsDouble(candidate) - target);
                if (Math.abs(value) < tolerance) {
                    return candidate;
                } else {
                    setDerivativeValue(derivative.applyAsDouble(candidate));
                    setCandidate(candidate - value / derivativeValue);
                }
            }
            throw new NonconvergenceException(guess, iterations);
        }
    }
}
