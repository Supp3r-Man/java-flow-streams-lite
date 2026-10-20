package com.github.sup3rmar10.flowstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import org.junit.jupiter.api.Test;

/** publishOn: bounded queue, demand replenishment, error ordering, rejection, and real-thread delivery. */
class PublishOnTest {
    static final RuntimeException BOOM = new RuntimeException("boom");

    private static ExecutorService singleThread(String name) {
        return Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        });
    }

    @Test
    void nothingIsDeliveredUntilTheExecutorRunsTheDrainTask() {
        ManualExecutor exec = new ManualExecutor();
        TestSubscriber<Long> ts = Tests.run(Source.range(0, 5).publishOn(exec, 4), Long.MAX_VALUE);
        assertTrue(ts.items.isEmpty());
        assertTrue(exec.pending() > 0);
        exec.runAll();
        assertEquals(Tests.longs(0, 5), ts.items);
        assertTrue(ts.completed);
        ts.assertClean();
    }

    @Test
    void upstreamIsAskedForExactlyTheCapacityNoMatterHowMuchDownstreamWants() {
        ManualExecutor exec = new ManualExecutor();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        Tests.run(Source.from(mp).publishOn(exec, 8), Long.MAX_VALUE);
        assertEquals(8, mp.requestedTotal.get());
        assertEquals(8, mp.outstanding());
    }

    @Test
    void upstreamIsToppedUpOnlyAfterItemsHaveBeenHandedDownstream() {
        ManualExecutor exec = new ManualExecutor();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).publishOn(exec, 8), 0);
        for (long i = 0; i < 8; i++) {
            mp.emit(i);
        }
        exec.runAll();
        assertEquals(8, mp.requestedTotal.get(), "no demand downstream: nothing consumed, nothing replenished");
        ts.request(5);
        exec.runAll();
        assertEquals(8, mp.requestedTotal.get(), "5 consumed < limit 6");
        ts.request(1);
        exec.runAll();
        assertEquals(14, mp.requestedTotal.get(), "6 consumed: topped up by 6");
        ts.assertClean();
    }

    @Test
    void anUpstreamThatOverflowsTheQueueFailsTheStreamInsteadOfGrowingIt() {
        ManualExecutor exec = new ManualExecutor();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).publishOn(exec, 4), Long.MAX_VALUE);
        for (long i = 0; i < 5; i++) {
            mp.emit(i); // the 5th ignores the demand of 4
        }
        exec.runAll();
        IllegalStateException ex = assertInstanceOf(IllegalStateException.class, ts.error);
        assertTrue(ex.getMessage().contains("overflow"));
        assertTrue(mp.cancelled);
        assertTrue(ts.items.isEmpty(), "queued items are dropped on a failure");
        ts.assertClean();
    }

    @Test
    void anUpstreamErrorArrivesAfterTheItemsThatWereAlreadyQueued() {
        ManualExecutor exec = new ManualExecutor();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).publishOn(exec, 4), 0);
        mp.emit(1L);
        mp.emit(2L);
        mp.fail(BOOM);
        exec.runAll();
        assertEquals(0, ts.terminalCount.get());
        ts.request(1);
        exec.runAll();
        assertEquals(List.of(1L), ts.items);
        assertEquals(0, ts.terminalCount.get());
        ts.request(1);
        exec.runAll();
        assertEquals(List.of(1L, 2L), ts.items);
        assertSame(BOOM, ts.error);
        ts.assertClean();
    }

    @Test
    void completionNeedsNoDemandOnceTheQueueIsEmpty() {
        ManualExecutor exec = new ManualExecutor();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).publishOn(exec, 4), 0);
        mp.complete();
        exec.runAll();
        assertTrue(ts.completed);
        ts.assertClean();
    }

    @Test
    void completionWaitsForQueuedItemsEvenWithoutDemand() {
        ManualExecutor exec = new ManualExecutor();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).publishOn(exec, 4), 0);
        mp.emit(1L);
        mp.complete();
        exec.runAll();
        assertEquals(0, ts.terminalCount.get());
        ts.request(1);
        exec.runAll();
        assertTrue(ts.completed);
        assertEquals(List.of(1L), ts.items);
    }

    @Test
    void cancelClearsTheQueueAndCancelsUpstream() {
        ManualExecutor exec = new ManualExecutor();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).publishOn(exec, 4), Long.MAX_VALUE);
        mp.emit(1L);
        mp.emit(2L);
        ts.cancel();
        assertTrue(mp.cancelled);
        exec.runAll();
        assertTrue(ts.items.isEmpty());
        mp.emit(3L);
        mp.complete();
        exec.runAll();
        assertEquals(0, ts.terminalCount.get());
        ts.assertClean();
    }

    @Test
    void invalidRequestFailsFastAndDropsQueuedItems() {
        ManualExecutor exec = new ManualExecutor();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).publishOn(exec, 4), 0);
        mp.emit(1L);
        ts.request(0);
        exec.runAll();
        assertInstanceOf(IllegalArgumentException.class, ts.error);
        assertTrue(ts.items.isEmpty());
        assertTrue(mp.cancelled);
    }

    @Test
    void aRejectedExecutionBecomesOnErrorAndStopsUpstream() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).publishOn(r -> {
            throw new RejectedExecutionException("full");
        }, 4), 0);
        ts.request(1);
        assertInstanceOf(RejectedExecutionException.class, ts.error);
        assertTrue(mp.cancelled);
        mp.emit(1L);
        assertEquals(1, ts.terminalCount.get());
        ts.assertClean();
    }

    @Test
    void signalsAreDeliveredOnTheExecutorThreadNotTheSubscribingOne() throws Exception {
        ExecutorService exec = singleThread("flow-test-exec");
        try {
            Set<String> threads = ConcurrentHashMap.newKeySet();
            TestSubscriber<Long> ts = new TestSubscriber<>(Long.MAX_VALUE);
            ts.hook(x -> threads.add(Thread.currentThread().getName()));
            Source.range(0, 100).publishOn(exec, 8).subscribe(ts);
            assertTrue(ts.awaitTerminal());
            assertEquals(Set.of("flow-test-exec"), threads);
            assertNotEquals("flow-test-exec", Thread.currentThread().getName());
            assertEquals(Tests.longs(0, 100), ts.items);
            ts.assertClean();
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    void argumentsAreValidated() {
        Source<Long> s = Source.range(0, 1);
        assertThrows(IllegalArgumentException.class, () -> s.publishOn(Subscriptions.DIRECT, 0));
        assertThrows(NullPointerException.class, () -> s.publishOn(null, 4));
    }
}
