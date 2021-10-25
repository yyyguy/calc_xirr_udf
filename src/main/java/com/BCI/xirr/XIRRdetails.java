package com.BCI.xirr;

import java.time.LocalDate;
import java.util.stream.Collector;

/**
 * Converts a stream of {@link Transaction} instances into the data needed for
 * the {@link XIRR} algorithm.
 */
class XIRRdetails {
    public static Collector<Transaction, XIRRdetails, XIRRdetails> collector() {
        return Collector.of(
                XIRRdetails::new,
                XIRRdetails::accumulate,
                XIRRdetails::combine,
                Collector.Characteristics.IDENTITY_FINISH,
                Collector.Characteristics.UNORDERED);
    }

    LocalDate start;
    LocalDate end;
    double minAmount = Double.POSITIVE_INFINITY;
    double maxAmount = Double.NEGATIVE_INFINITY;
    double total;
    double deposits;

    public void accumulate(final Transaction tx) {
        start = start != null && start.isBefore(tx.when) ? start : tx.when;
        end = end != null && end.isAfter(tx.when) ? end : tx.when;
        minAmount = Math.min(minAmount, tx.amount);
        maxAmount = Math.max(maxAmount, tx.amount);
        total += tx.amount;
        if (tx.amount < 0) {
            deposits -= tx.amount;
        }
    }

    public XIRRdetails combine(final XIRRdetails other) {
        start = start.isBefore(other.start) ? start : other.start;
        end = end.isAfter(other.end) ? end : other.end;
        minAmount = Math.min(minAmount, other.minAmount);
        maxAmount = Math.max(maxAmount, other.maxAmount);
        total += other.total;
        return this;
    }

    public void validate() {
        if (start == null) {
            throw new IllegalArgumentException("No transactions to analyze.");
        }

        if (start.equals(end)) {
            throw new IllegalArgumentException(
                    "Transactions must not all be on the same day.");
        }
        if (minAmount >= 0) {
            throw new IllegalArgumentException(
                    "Transactions must not all be non-negative.");
        }
        if (maxAmount < 0) {
            throw new IllegalArgumentException(
                    "Transactions must not be negative.");
        }
    }

}
