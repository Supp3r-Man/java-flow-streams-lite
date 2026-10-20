package com.github.sup3rmar10.flowstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class DemandTest {

    @Test
    void additionSaturatesAtLongMaxValueInsteadOfOverflowing() {
        assertEquals(11, Demand.add(5, 6));
        assertEquals(0, Demand.add(0, 0));
        assertEquals(Long.MAX_VALUE, Demand.add(Long.MAX_VALUE - 1, 10));
        assertEquals(Long.MAX_VALUE, Demand.add(Long.MAX_VALUE, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, Demand.add(Long.MAX_VALUE, 1));
    }

    @Test
    void addCapReturnsThePreviousValue() {
        AtomicLong r = new AtomicLong();
        assertEquals(0, Demand.addCap(r, 3));
        assertEquals(3, Demand.addCap(r, 4));
        assertEquals(7, r.get());
    }

    @Test
    void addCapSaturatesAndStaysUnbounded() {
        AtomicLong r = new AtomicLong(Long.MAX_VALUE - 5);
        assertEquals(Long.MAX_VALUE - 5, Demand.addCap(r, 100));
        assertEquals(Long.MAX_VALUE, r.get());
        assertEquals(Long.MAX_VALUE, Demand.addCap(r, 1));
        assertEquals(Long.MAX_VALUE, r.get());
    }

    @Test
    void multiplySaturates() {
        assertEquals(12, Demand.multiply(3, 4));
        assertEquals(Long.MAX_VALUE, Demand.multiply(Long.MAX_VALUE, 1));
        assertEquals(Long.MAX_VALUE, Demand.multiply(Long.MAX_VALUE / 2, 3));
        assertEquals(Long.MAX_VALUE, Demand.multiply(1L << 62, 2)); // low word negative
        assertEquals(Long.MAX_VALUE, Demand.multiply(1L << 32, 1L << 32)); // low word zero, high word 1
    }

    @Test
    void invalidRequestErrorNamesTheRule() {
        IllegalArgumentException e = Demand.invalidRequest(-3);
        assertTrue(e.getMessage().contains("3.9"));
        assertTrue(e.getMessage().contains("-3"));
    }

    @Test
    void concurrentAdditionsLoseNothing() throws Exception {
        AtomicLong r = new AtomicLong();
        int threads = 8;
        int each = 5_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int t = 0; t < threads; t++) {
                pool.execute(() -> {
                    try {
                        go.await();
                        for (int i = 0; i < each; i++) {
                            Demand.addCap(r, 1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    done.countDown();
                });
            }
            go.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
        assertEquals((long) threads * each, r.get());
    }
}
