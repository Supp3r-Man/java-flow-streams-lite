package com.github.sup3rmar10.flowstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Real threads. These tests assert invariants (serial signals, never more than requested, exactly-once delivery,
 * per-source order, bounded run-ahead), never timings. They wait on latches and futures, never on sleeps.
 */
class ConcurrencyTest {

    private static ExecutorService pool(int n) {
        return Executors.newFixedThreadPool(n, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
    }

    private static void all(List<Future<?>> fs) throws Exception {
        for (Future<?> f : fs) {
            f.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void requestsFromManyThreadsAreAllHonouredExactlyOnceAndInOrder() throws Exception {
        TestSubscriber<Long> ts = Tests.run(Source.range(0, 100_000), 0);
        ExecutorService pool = pool(4);
        try {
            CyclicBarrier go = new CyclicBarrier(4);
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                fs.add(pool.submit(() -> {
                    go.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < 25; i++) {
                        ts.request(1000);
                    }
                    return null;
                }));
            }
            all(fs);
        } finally {
            pool.shutdownNow();
        }
        assertTrue(ts.completed);
        assertEquals(Tests.longs(0, 100_000), ts.items);
        ts.assertClean();
    }

    @Test
    void requestRacingCancelNeverBreaksSerializationOrOverDelivers() throws Exception {
        ExecutorService pool = pool(2);
        try {
            for (int round = 0; round < 200; round++) {
                TestSubscriber<Long> ts = Tests.run(Source.range(0, 1000), 0).lenientAfterCancel();
                CyclicBarrier go = new CyclicBarrier(2);
                Future<?> a = pool.submit(() -> {
                    go.await(10, TimeUnit.SECONDS);
                    ts.request(Long.MAX_VALUE);
                    return null;
                });
                Future<?> b = pool.submit(() -> {
                    go.await(10, TimeUnit.SECONDS);
                    ts.cancel();
                    return null;
                });
                all(List.of(a, b));
                assertTrue(ts.terminalCount.get() <= 1);
                assertEquals(Tests.longs(0, ts.items.size()), ts.items, "items must be an in-order prefix");
                ts.assertClean();
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private static void assertPerInnerOrder(List<Long> items, int inners, int perInner) {
        assertEquals(inners * perInner, items.size());
        for (int x = 0; x < inners; x++) {
            List<Long> mine = new ArrayList<>();
            for (Long v : items) {
                if (v / 100 == x) {
                    mine.add(v);
                }
            }
            assertEquals(Tests.longs(x * 100L, perInner), mine, "inner " + x);
        }
    }

    @Test
    void flatMapOverAsynchronousInnersDeliversEverythingSeriallyAndPerInnerInOrder() throws Exception {
        ExecutorService pool = pool(4);
        try {
            Source<Long> s = Source.range(0, 50).flatMap(x -> Source.range(x * 100, 20).publishOn(pool, 4), 5);
            TestSubscriber<Long> ts = Tests.run(s, Long.MAX_VALUE);
            assertTrue(ts.awaitTerminal());
            assertTrue(ts.completed, "error: " + ts.error);
            assertPerInnerOrder(ts.items, 50, 20);
            ts.assertClean();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void flatMapWithADemandOfOneAtATimeNeverOverDeliversAcrossThreads() throws Exception {
        ExecutorService pool = pool(4);
        try {
            Source<Long> s = Source.range(0, 50).flatMap(x -> Source.range(x * 100, 20).publishOn(pool, 4), 5);
            TestSubscriber<Long> ts = new TestSubscriber<>(1);
            ts.hook(x -> ts.request(1));
            s.subscribe(ts);
            assertTrue(ts.awaitTerminal());
            assertTrue(ts.completed, "error: " + ts.error);
            assertPerInnerOrder(ts.items, 50, 20);
            ts.assertClean();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void mergeOfTwoAsynchronousSourcesKeepsEachOnesOrder() throws Exception {
        ExecutorService pool = pool(3);
        try {
            Source<Long> s = Source.merge(Source.range(0, 1000).publishOn(pool, 4), Source.range(1000, 1000).publishOn(pool, 4));
            TestSubscriber<Long> ts = Tests.run(s, Long.MAX_VALUE);
            assertTrue(ts.awaitTerminal());
            assertTrue(ts.completed, "error: " + ts.error);
            List<Long> a = new ArrayList<>();
            List<Long> b = new ArrayList<>();
            for (Long v : ts.items) {
                (v < 1000 ? a : b).add(v);
            }
            assertEquals(Tests.longs(0, 1000), a);
            assertEquals(Tests.longs(1000, 1000), b);
            ts.assertClean();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void aSlowConsumerNeverLetsTheProducerRunMoreThanCapacityAhead() throws Exception {
        ExecutorService pool = pool(1);
        try {
            final int capacity = 16;
            AtomicLong produced = new AtomicLong();
            AtomicLong consumed = new AtomicLong();
            AtomicLong maxAhead = new AtomicLong();
            Source<Long> s = Source.range(0, 20_000).map(x -> {
                long p = produced.incrementAndGet();
                maxAhead.accumulateAndGet(p - consumed.get(), Math::max);
                return x;
            }).publishOn(pool, capacity);
            TestSubscriber<Long> ts = new TestSubscriber<>(1);
            ts.hook(x -> {
                consumed.incrementAndGet();
                ts.request(1);
            });
            s.subscribe(ts);
            assertTrue(ts.awaitTerminal());
            assertTrue(ts.completed);
            assertEquals(20_000, ts.items.size());
            assertTrue(maxAhead.get() <= capacity, "producer ran " + maxAhead.get() + " ahead, capacity " + capacity);
            ts.assertClean();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void theSerializerRunsTasksOneAtATimeAndLosesNone() throws Exception {
        Serializer serial = new Serializer();
        int threads = 8;
        int each = 2000;
        int[] counter = {0};
        AtomicInteger active = new AtomicInteger();
        AtomicBoolean overlapped = new AtomicBoolean();
        ExecutorService pool = pool(threads);
        try {
            CyclicBarrier go = new CyclicBarrier(threads);
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                fs.add(pool.submit(() -> {
                    go.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < each; i++) {
                        serial.execute(() -> {
                            if (active.incrementAndGet() != 1) {
                                overlapped.set(true);
                            }
                            counter[0]++; // deliberately not atomic: only correct if tasks never overlap
                            active.decrementAndGet();
                        });
                    }
                    return null;
                }));
            }
            all(fs);
        } finally {
            pool.shutdownNow();
        }
        assertFalse(overlapped.get());
        assertEquals(threads * each, counter[0]);
    }

    @Test
    void aTaskSubmittedFromInsideATaskRunsAfterItNotNestedInIt() {
        Serializer serial = new Serializer();
        List<String> log = new ArrayList<>();
        serial.execute(() -> {
            log.add("outer-start");
            serial.execute(() -> log.add("inner"));
            log.add("outer-end");
        });
        assertEquals(List.of("outer-start", "outer-end", "inner"), log);
    }

    @Test
    void theRealSchedulerAdapterFiresATimeoutOnAnotherThread() throws Exception {
        ScheduledExecutorService timers = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        try {
            TestSubscriber<Long> ts = Tests.run(Source.<Long>never().timeout(Duration.ofMillis(20), Scheduler.from(timers)), 1);
            assertTrue(ts.awaitTerminal());
            assertInstanceOf(TimeoutException.class, ts.error);
            ts.assertClean();
        } finally {
            timers.shutdownNow();
        }
    }
}
