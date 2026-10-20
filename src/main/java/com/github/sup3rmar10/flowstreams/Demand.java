package com.github.sup3rmar10.flowstreams;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Demand arithmetic. Reactive Streams treats {@code Long.MAX_VALUE} as "unbounded" (rule 3.17), so every
 * addition saturates at that value instead of wrapping around to a negative number.
 */
final class Demand {
    private Demand() {
    }

    /** Saturating addition of two non-negative demands. */
    static long add(long current, long n) {
        long r = current + n;
        return r < 0 ? Long.MAX_VALUE : r;
    }

    /** Saturating multiplication of two positive numbers. */
    static long multiply(long a, long b) {
        long hi = Math.multiplyHigh(a, b);
        long lo = a * b;
        return (hi != 0 || lo < 0) ? Long.MAX_VALUE : lo;
    }

    /**
     * Atomically adds {@code n} to the counter (saturating) and returns the PREVIOUS value. A return of 0 means the
     * caller just moved the counter from "no demand" to "some demand" and therefore owns the emission loop.
     */
    static long addCap(AtomicLong requested, long n) {
        for (;;) {
            long cur = requested.get();
            if (cur == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            if (requested.compareAndSet(cur, add(cur, n))) {
                return cur;
            }
        }
    }

    static IllegalArgumentException invalidRequest(long n) {
        return new IllegalArgumentException("Reactive Streams 3.9: request(n) requires n > 0 but was " + n);
    }
}
