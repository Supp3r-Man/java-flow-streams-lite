package com.github.sup3rmar10.flowstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** onErrorResume (demand hand-over) and timeout (virtual time, demand-aware). */
class ResumeTimeoutTest {
    static final RuntimeException BOOM = new RuntimeException("boom");
    static final RuntimeException BOOM2 = new RuntimeException("boom2");

    // ------------------------------------------------------------------ onErrorResume

    @Test
    void theFallbackFunctionIsNotCalledWhenThereIsNoError() {
        AtomicInteger calls = new AtomicInteger();
        List<Long> out = Tests.collect(Source.range(0, 3).onErrorResume(e -> {
            calls.incrementAndGet();
            return Source.just(99L);
        }));
        assertEquals(List.of(0L, 1L, 2L), out);
        assertEquals(0, calls.get());
    }

    @Test
    void errorSwitchesToTheFallbackAndKeepsOrder() {
        AtomicReference<Throwable> seen = new AtomicReference<>();
        Source<Long> s = Source.range(0, 3).concatWith(Source.<Long>error(BOOM)).onErrorResume(e -> {
            seen.set(e);
            return Source.just(100L, 200L);
        });
        assertEquals(List.of(0L, 1L, 2L, 100L, 200L), Tests.collect(s));
        assertSame(BOOM, seen.get());
    }

    @Test
    void outstandingDemandIsCarriedToTheFallback() {
        ManualPublisher<Long> primary = new ManualPublisher<>();
        ManualPublisher<Long> fallback = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(primary).onErrorResume(e -> fallback), 5);
        primary.emit(1L);
        primary.emit(2L);
        primary.fail(BOOM);
        assertEquals(1, fallback.subscribeCount.get());
        assertEquals(3, fallback.requestedTotal.get(), "5 requested - 2 delivered");
        fallback.emit(3L);
        fallback.emit(4L);
        fallback.emit(5L);
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), ts.items);
        ts.assertClean();
    }

    @Test
    void unboundedDemandStaysUnboundedOnTheFallback() {
        ManualPublisher<Long> primary = new ManualPublisher<>();
        ManualPublisher<Long> fallback = new ManualPublisher<>();
        Tests.run(Source.from(primary).onErrorResume(e -> fallback), Long.MAX_VALUE);
        primary.emit(1L);
        primary.fail(BOOM);
        assertEquals(Long.MAX_VALUE, fallback.requestedTotal.get());
    }

    @Test
    void noDemandMeansNoRequestUntilDownstreamAsks() {
        ManualPublisher<Long> primary = new ManualPublisher<>();
        ManualPublisher<Long> fallback = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(primary).onErrorResume(e -> fallback), 0);
        primary.fail(BOOM);
        assertEquals(1, fallback.subscribeCount.get());
        assertEquals(0, fallback.requestedTotal.get());
        ts.request(2);
        assertEquals(2, fallback.requestedTotal.get(), "requests after the switch go to the fallback");
        assertEquals(0, primary.requestedTotal.get());
    }

    @Test
    void demandAccountingSurvivesTwoSwitchesInARow() {
        ManualPublisher<Long> p1 = new ManualPublisher<>();
        ManualPublisher<Long> p2 = new ManualPublisher<>();
        ManualPublisher<Long> p3 = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(
                Source.from(p1).onErrorResume(e -> p2).onErrorResume(e -> p3), 4);
        p1.emit(1L);
        p1.fail(BOOM);
        assertEquals(3, p2.requestedTotal.get());
        p2.emit(2L);
        p2.fail(BOOM2);
        assertEquals(2, p3.requestedTotal.get());
        ts.assertClean();
    }

    @Test
    void anErrorFromTheFallbackIsNotResumedAgain() {
        TestSubscriber<Long> ts = Tests.run(
                Source.<Long>error(BOOM).onErrorResume(e -> Source.<Long>error(BOOM2)), Long.MAX_VALUE);
        assertSame(BOOM2, ts.error);
        assertEquals(1, ts.terminalCount.get());
    }

    @Test
    void aThrowingFallbackFunctionReportsItsOwnErrorWithTheOriginalSuppressed() {
        RuntimeException mine = new RuntimeException("fallback exploded");
        TestSubscriber<Long> ts = Tests.run(Source.<Long>error(BOOM).onErrorResume(e -> {
            throw mine;
        }), 1);
        assertSame(mine, ts.error);
        assertEquals(List.of(BOOM), List.of(mine.getSuppressed()));
    }

    @Test
    void aNullFallbackIsANullPointerError() {
        TestSubscriber<Long> ts = Tests.run(Source.<Long>error(BOOM).onErrorResume(e -> null), 1);
        assertInstanceOf(NullPointerException.class, ts.error);
    }

    @Test
    void cancelBeforeTheSwitchCancelsPrimaryAndTheFallbackIsNeverUsed() {
        ManualPublisher<Long> primary = new ManualPublisher<>();
        AtomicInteger calls = new AtomicInteger();
        TestSubscriber<Long> ts = Tests.run(Source.from(primary).onErrorResume(e -> {
            calls.incrementAndGet();
            return Source.just(1L);
        }), 1);
        ts.cancel();
        assertTrue(primary.cancelled);
        primary.fail(BOOM);
        assertEquals(0, calls.get());
        assertEquals(0, ts.terminalCount.get());
        ts.assertClean();
    }

    @Test
    void cancelAfterTheSwitchCancelsTheFallback() {
        ManualPublisher<Long> primary = new ManualPublisher<>();
        ManualPublisher<Long> fallback = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(primary).onErrorResume(e -> fallback), 1);
        primary.fail(BOOM);
        ts.cancel();
        assertTrue(fallback.cancelled);
        fallback.emit(1L);
        assertTrue(ts.items.isEmpty());
        ts.assertClean();
    }

    @Test
    void anInvalidRequestIsNotSwallowedByTheFallback() {
        AtomicInteger calls = new AtomicInteger();
        TestSubscriber<Long> ts = Tests.run(Source.range(0, 3).onErrorResume(e -> {
            calls.incrementAndGet();
            return Source.just(42L);
        }), 0);
        ts.request(0);
        assertInstanceOf(IllegalArgumentException.class, ts.error);
        assertEquals(0, calls.get());
        assertTrue(ts.items.isEmpty());
    }

    // ------------------------------------------------------------------ timeout

    private static final Duration SEC = Duration.ofSeconds(1);

    @Test
    void firesExactlyAtTheTimeoutWhileDemandIsOutstanding() {
        TestScheduler clock = new TestScheduler();
        TestSubscriber<Long> ts = Tests.run(Source.<Long>never().timeout(SEC, clock), 1);
        clock.advanceBy(Duration.ofMillis(999));
        assertEquals(0, ts.terminalCount.get());
        clock.advanceBy(Duration.ofMillis(1));
        TimeoutException te = assertInstanceOf(TimeoutException.class, ts.error);
        assertTrue(te.getMessage().contains("1000 ms"));
        assertTrue(te.getMessage().contains("waited 1000 ms"));
        ts.assertClean();
    }

    @Test
    void noDemandMeansNoTimer() {
        TestScheduler clock = new TestScheduler();
        TestSubscriber<Long> ts = Tests.run(Source.<Long>never().timeout(SEC, clock), 0);
        assertEquals(0, clock.pendingTasks());
        clock.advanceBy(Duration.ofHours(1));
        assertEquals(0, ts.terminalCount.get());
    }

    @Test
    void theTimerStartsWhenDemandArrivesNotWhenSubscribed() {
        TestScheduler clock = new TestScheduler();
        TestSubscriber<Long> ts = Tests.run(Source.<Long>never().timeout(SEC, clock), 0);
        clock.advanceBy(Duration.ofSeconds(5));
        ts.request(1);
        assertEquals(1, clock.pendingTasks());
        clock.advanceBy(Duration.ofMillis(999));
        assertEquals(0, ts.terminalCount.get());
        clock.advanceBy(Duration.ofMillis(1));
        assertInstanceOf(TimeoutException.class, ts.error);
    }

    @Test
    void eachItemRestartsTheTimer() {
        TestScheduler clock = new TestScheduler();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).timeout(Duration.ofMillis(100), clock), Long.MAX_VALUE);
        for (int i = 0; i < 50; i++) {
            clock.advanceBy(Duration.ofMillis(99));
            mp.emit((long) i);
        }
        assertEquals(0, ts.terminalCount.get(), "items every 99 ms never hit the 100 ms timeout");
        assertEquals(50, ts.items.size());
        clock.advanceBy(Duration.ofMillis(99));
        assertEquals(0, ts.terminalCount.get());
        clock.advanceBy(Duration.ofMillis(1));
        assertInstanceOf(TimeoutException.class, ts.error);
        assertEquals(50, ts.items.size());
        ts.assertClean();
    }

    @Test
    void timerStopsWhenAnItemUsesUpTheDemandAndRestartsOnTheNextRequest() {
        TestScheduler clock = new TestScheduler();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).timeout(SEC, clock), 1);
        mp.emit(1L);
        assertEquals(0, clock.pendingTasks());
        clock.advanceBy(Duration.ofHours(1));
        assertEquals(0, ts.terminalCount.get());
        ts.request(1);
        assertEquals(1, clock.pendingTasks());
    }

    @Test
    void completionDisarmsTheTimer() {
        TestScheduler clock = new TestScheduler();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).timeout(SEC, clock), 5);
        assertEquals(1, clock.pendingTasks());
        mp.complete();
        assertEquals(0, clock.pendingTasks());
        assertTrue(ts.completed);
        clock.advanceBy(Duration.ofHours(1));
        assertEquals(1, ts.terminalCount.get());
    }

    @Test
    void upstreamErrorDisarmsTheTimerAndIsForwarded() {
        TestScheduler clock = new TestScheduler();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).timeout(SEC, clock), 5);
        mp.fail(BOOM);
        assertEquals(0, clock.pendingTasks());
        assertSame(BOOM, ts.error);
    }

    @Test
    void cancelDisarmsTheTimerAndCancelsUpstream() {
        TestScheduler clock = new TestScheduler();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).timeout(SEC, clock), 5);
        ts.cancel();
        assertTrue(mp.cancelled);
        assertEquals(0, clock.pendingTasks());
        clock.advanceBy(Duration.ofHours(1));
        assertEquals(0, ts.terminalCount.get());
    }

    @Test
    void aTimeoutCancelsUpstreamAndLateSignalsAreDropped() {
        TestScheduler clock = new TestScheduler();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).timeout(SEC, clock), 5);
        clock.advanceBy(SEC);
        assertTrue(mp.cancelled);
        mp.emit(1L);
        mp.complete();
        assertTrue(ts.items.isEmpty());
        assertEquals(1, ts.terminalCount.get());
        ts.assertClean();
    }

    @Test
    void theTimeoutAfterAnItemIsMeasuredFromThatItem() {
        TestScheduler clock = new TestScheduler();
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).timeout(SEC, clock), 5);
        clock.advanceBy(Duration.ofMillis(400));
        mp.emit(1L);
        clock.advanceBy(Duration.ofMillis(999));
        assertFalse(ts.terminalCount.get() > 0);
        clock.advanceBy(Duration.ofMillis(1));
        TimeoutException te = assertInstanceOf(TimeoutException.class, ts.error);
        assertTrue(te.getMessage().contains("waited 1000 ms"));
    }

    // Added after the first mutation run: M21 survived. TestScheduler cancels timers synchronously, so a stale timer
    // never fired; a real scheduler can fire a timer that is already queued while an item is being processed.
    @Test
    void aStaleTimerThatCouldNotBeCancelledInTimeIsIgnored() {
        java.util.List<Runnable> captured = new java.util.ArrayList<>();
        Scheduler uncancellable = new Scheduler() {
            @Override
            public long nanoTime() {
                return 0;
            }

            @Override
            public Handle schedule(Runnable task, long delay, java.util.concurrent.TimeUnit unit) {
                captured.add(task);
                return () -> { };
            }
        };
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).timeout(SEC, uncancellable), Long.MAX_VALUE);
        assertEquals(1, captured.size());
        mp.emit(1L); // the item wins the race; the first timer is now stale
        captured.get(0).run(); // ... and it fires anyway
        assertEquals(0, ts.terminalCount.get());
        assertFalse(mp.cancelled);
        assertEquals(2, captured.size(), "a fresh timer was armed after the item");
        captured.get(1).run();
        assertInstanceOf(TimeoutException.class, ts.error);
        ts.assertClean();
    }

    @Test
    void argumentsAreValidated() {
        TestScheduler clock = new TestScheduler();
        Source<Long> s = Source.range(0, 1);
        assertThrows(IllegalArgumentException.class, () -> s.timeout(Duration.ZERO, clock));
        assertThrows(IllegalArgumentException.class, () -> s.timeout(Duration.ofMillis(-5), clock));
        assertThrows(NullPointerException.class, () -> s.timeout(SEC, null));
        assertThrows(NullPointerException.class, () -> s.timeout(null, clock));
    }
}
