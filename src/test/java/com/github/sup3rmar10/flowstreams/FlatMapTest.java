package com.github.sup3rmar10.flowstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/** flatMap, merge and concat: bounded concurrency, bounded prefetch, fail-fast, demand, ordering. */
class FlatMapTest {
    static final RuntimeException BOOM = new RuntimeException("boom");

    /** Drives flatMap by hand: the outer and every inner are ManualPublishers. */
    static final class Rig {
        final ManualPublisher<Long> outer = new ManualPublisher<>();
        final List<ManualPublisher<Long>> inners = new CopyOnWriteArrayList<>();
        final TestSubscriber<Long> ts;

        Rig(int maxConcurrency, int prefetch, long request) {
            Source<Long> out = Source.from(outer).<Long>flatMap(x -> {
                ManualPublisher<Long> p = new ManualPublisher<>();
                inners.add(p);
                return p;
            }, maxConcurrency, prefetch);
            ts = Tests.run(out, request);
        }

        long subscribedAndNotCancelledInners() {
            return inners.stream().filter(p -> p.subscribeCount.get() == 1 && !p.cancelled).count();
        }
    }

    @Test
    void outerIsAskedForExactlyMaxConcurrencyUpFront() {
        Rig rig = new Rig(3, 16, Long.MAX_VALUE);
        assertEquals(3, rig.outer.requestedTotal.get());
    }

    @Test
    void aFinishedInnerFreesExactlyOneSlot() {
        Rig rig = new Rig(3, 16, Long.MAX_VALUE);
        for (long i = 0; i < 3; i++) {
            rig.outer.emit(i);
        }
        assertEquals(3, rig.inners.size());
        assertEquals(3, rig.subscribedAndNotCancelledInners());
        rig.inners.get(0).complete();
        assertEquals(4, rig.outer.requestedTotal.get());
        rig.outer.emit(3L);
        assertEquals(4, rig.inners.size());
        rig.inners.get(1).complete();
        rig.inners.get(2).complete();
        assertEquals(6, rig.outer.requestedTotal.get());
        rig.ts.assertClean();
    }

    @Test
    void concurrencyNeverExceedsTheBoundWhileRunningARealPipeline() {
        List<ManualPublisher<Long>> inners = new ArrayList<>();
        Source<Long> s = Source.range(0, 20).<Long>flatMap(x -> {
            ManualPublisher<Long> p = new ManualPublisher<>();
            inners.add(p);
            return p;
        }, 4);
        TestSubscriber<Long> ts = Tests.run(s, Long.MAX_VALUE);
        // the synchronous outer could have produced all 20, but only 4 inners may exist before one completes
        assertEquals(4, inners.size());
        int done = 0;
        while (done < 20) {
            inners.get(done).complete();
            done++;
            assertEquals(Math.min(20, 4 + done), inners.size(), "after " + done + " completions");
            assertTrue(inners.size() - done <= 4, "more than 4 inners active after " + done + " completions");
        }
        assertEquals(20, inners.size());
        assertTrue(ts.completed);
        ts.assertClean();
    }

    @Test
    void eachInnerIsOnlyAskedForItsPrefetchAndToppedUpAsItemsAreConsumed() {
        Rig rig = new Rig(1, 4, 0);
        rig.outer.emit(0L);
        ManualPublisher<Long> in = rig.inners.get(0);
        assertEquals(4, in.requestedTotal.get());
        for (long i = 0; i < 4; i++) {
            in.emit(i); // queued: downstream demand is 0
        }
        assertTrue(rig.ts.items.isEmpty());
        rig.ts.request(2);
        assertEquals(2, rig.ts.items.size());
        assertEquals(4, in.requestedTotal.get(), "limit is 3 consumed items, not yet reached");
        rig.ts.request(1);
        assertEquals(7, in.requestedTotal.get(), "after 3 consumed the inner is topped up by 3");
        rig.ts.assertClean();
    }

    @Test
    void downstreamDemandIsNeverExceeded() {
        Rig rig = new Rig(2, 16, 1);
        rig.outer.emit(0L);
        ManualPublisher<Long> in = rig.inners.get(0);
        for (long i = 1; i <= 4; i++) {
            in.emit(i);
        }
        assertEquals(List.of(1L), rig.ts.items);
        rig.ts.request(2);
        assertEquals(List.of(1L, 2L, 3L), rig.ts.items);
        rig.ts.assertClean();
    }

    @Test
    void completionWaitsUntilAllQueuedItemsAreDelivered() {
        Rig rig = new Rig(2, 16, 0);
        rig.outer.emit(0L);
        rig.inners.get(0).emit(10L);
        rig.inners.get(0).emit(11L);
        rig.inners.get(0).complete();
        rig.outer.complete();
        assertEquals(0, rig.ts.terminalCount.get());
        rig.ts.request(1);
        assertEquals(0, rig.ts.terminalCount.get());
        rig.ts.request(1);
        assertTrue(rig.ts.completed);
        assertEquals(List.of(10L, 11L), rig.ts.items);
        rig.ts.assertClean();
    }

    @Test
    void concatKeepsOrderAndSubscribesToTheNextSourceLazily() {
        ManualPublisher<Long> a = new ManualPublisher<>();
        ManualPublisher<Long> b = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.concat(Source.from(a), Source.from(b)), Long.MAX_VALUE);
        assertEquals(1, a.subscribeCount.get());
        assertEquals(0, b.subscribeCount.get());
        a.emit(1L);
        a.emit(2L);
        a.complete();
        assertEquals(1, b.subscribeCount.get());
        b.emit(3L);
        b.complete();
        assertEquals(List.of(1L, 2L, 3L), ts.items);
        assertTrue(ts.completed);
        ts.assertClean();
    }

    @Test
    void concatStopsAtTheFirstError() {
        ManualPublisher<Long> b = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(
                Source.concat(Source.range(0, 2), Source.<Long>error(BOOM), Source.from(b)), Long.MAX_VALUE);
        assertEquals(List.of(0L, 1L), ts.items);
        assertSame(BOOM, ts.error);
        assertEquals(0, b.subscribeCount.get());
    }

    @Test
    void innerErrorIsFailFast_dropsQueuedItemsAndCancelsEverythingElse() {
        Rig rig = new Rig(2, 16, 0);
        rig.outer.emit(0L);
        rig.outer.emit(1L);
        rig.inners.get(0).emit(10L); // queued, no demand
        rig.inners.get(1).fail(BOOM);
        assertSame(BOOM, rig.ts.error);
        assertTrue(rig.ts.items.isEmpty());
        assertTrue(rig.inners.get(0).cancelled);
        assertTrue(rig.outer.cancelled);
        assertEquals(1, rig.ts.terminalCount.get());
        rig.ts.assertClean();
    }

    @Test
    void outerErrorCancelsInners() {
        Rig rig = new Rig(2, 16, Long.MAX_VALUE);
        rig.outer.emit(0L);
        rig.outer.fail(BOOM);
        assertSame(BOOM, rig.ts.error);
        assertTrue(rig.inners.get(0).cancelled);
        rig.ts.assertClean();
    }

    @Test
    void mapperExceptionCancelsOuterAndInners() {
        ManualPublisher<Long> outer = new ManualPublisher<>();
        List<ManualPublisher<Long>> inners = new ArrayList<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(outer).<Long>flatMap(x -> {
            if (x == 2L) {
                throw BOOM;
            }
            ManualPublisher<Long> p = new ManualPublisher<>();
            inners.add(p);
            return p;
        }, 3), Long.MAX_VALUE);
        outer.emit(1L);
        outer.emit(2L);
        assertSame(BOOM, ts.error);
        assertTrue(outer.cancelled);
        assertTrue(inners.get(0).cancelled);
        ts.assertClean();
    }

    @Test
    void mapperReturningNullIsANullPointerError() {
        TestSubscriber<Long> ts = Tests.run(Source.range(0, 3).<Long>flatMap(x -> null, 2), Long.MAX_VALUE);
        assertInstanceOf(NullPointerException.class, ts.error);
    }

    @Test
    void anInnerEmittingNullFailsTheStream() {
        Rig rig = new Rig(1, 4, Long.MAX_VALUE);
        rig.outer.emit(0L);
        rig.inners.get(0).emit(null);
        assertInstanceOf(NullPointerException.class, rig.ts.error);
        assertTrue(rig.inners.get(0).cancelled);
    }

    @Test
    void cancelCancelsOuterAndAllInnersAndSilencesLateSignals() {
        Rig rig = new Rig(2, 16, Long.MAX_VALUE);
        rig.outer.emit(0L);
        rig.outer.emit(1L);
        rig.ts.cancel();
        assertTrue(rig.outer.cancelled);
        assertTrue(rig.inners.get(0).cancelled);
        assertTrue(rig.inners.get(1).cancelled);
        rig.inners.get(0).emit(5L);
        rig.inners.get(0).complete();
        rig.outer.complete();
        assertTrue(rig.ts.items.isEmpty());
        assertEquals(0, rig.ts.terminalCount.get());
        rig.ts.assertClean();
    }

    @Test
    void invalidRequestCancelsEverything() {
        Rig rig = new Rig(2, 16, 0);
        rig.outer.emit(0L);
        rig.ts.request(0);
        assertInstanceOf(IllegalArgumentException.class, rig.ts.error);
        assertTrue(rig.outer.cancelled);
        assertTrue(rig.inners.get(0).cancelled);
    }

    @Test
    void emptyInnersJustComplete() {
        assertEquals(List.of(), Tests.collect(Source.range(0, 5).<Long>flatMap(x -> Source.empty(), 2)));
    }

    @Test
    void concurrencyOneRecyclesItsSingleSlotInOrder() {
        List<Long> out = Tests.collect(Source.range(0, 1000).flatMap(x -> Source.just(x), 1));
        assertEquals(Tests.longs(0, 1000), out);
    }

    @Test
    void mergeDeliversEverythingAndKeepsEachSourcesOwnOrder() {
        List<Long> out = Tests.collect(Source.merge(Source.range(0, 40), Source.range(100, 40), Source.range(200, 40)));
        assertEquals(120, out.size());
        for (long base : new long[] {0, 100, 200}) {
            List<Long> mine = new ArrayList<>();
            for (Long v : out) {
                if (v >= base && v < base + 40) {
                    mine.add(v);
                }
            }
            assertEquals(Tests.longs(base, 40), mine);
        }
    }

    @Test
    void mergeAndConcatOfNothingAreEmpty() {
        assertEquals(List.of(), Tests.collect(Source.<Long>merge()));
        assertEquals(List.of(), Tests.collect(Source.<Long>concat()));
    }

    @Test
    void argumentsAreValidated() {
        Source<Long> s = Source.range(0, 1);
        assertThrows(IllegalArgumentException.class, () -> s.flatMap(x -> Source.just(x), 0));
        assertThrows(IllegalArgumentException.class, () -> s.flatMap(x -> Source.just(x), 1, 0));
        assertThrows(NullPointerException.class, () -> s.flatMap(null, 1));
    }

    // Added after the first mutation run: M25 survived (merge degraded to concat unnoticed).
    @Test
    void mergeSubscribesToEverySourceImmediatelyUnlikeConcat() {
        ManualPublisher<Long> a = new ManualPublisher<>();
        ManualPublisher<Long> b = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.merge(Source.from(a), Source.from(b)), Long.MAX_VALUE);
        assertEquals(1, a.subscribeCount.get());
        assertEquals(1, b.subscribeCount.get());
        b.emit(2L);
        a.emit(1L);
        assertEquals(List.of(2L, 1L), ts.items);
        a.complete();
        b.complete();
        assertTrue(ts.completed);
        ts.assertClean();
    }

    // Added after the first mutation run: M17 survived (late onSubscribe after cancel was never exercised).
    @Test
    void anInnerThatSubscribesLateAfterCancelIsCancelledImmediately() {
        ManualPublisher<Long> outer = new ManualPublisher<>();
        java.util.concurrent.atomic.AtomicReference<java.util.concurrent.Flow.Subscriber<? super Long>> captured =
                new java.util.concurrent.atomic.AtomicReference<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(outer).<Long>flatMap(
                x -> (java.util.concurrent.Flow.Publisher<Long>) s -> captured.set(s), 1), Long.MAX_VALUE);
        outer.emit(0L);
        ts.cancel();
        long[] requested = {0};
        boolean[] cancelled = {false};
        captured.get().onSubscribe(new java.util.concurrent.Flow.Subscription() {
            @Override
            public void request(long n) {
                requested[0] += n;
            }

            @Override
            public void cancel() {
                cancelled[0] = true;
            }
        });
        assertTrue(cancelled[0], "a subscription that arrives after cancel() must be cancelled");
        assertEquals(0, requested[0]);
    }

    @Test
    void roundRobinAcrossInnersWithDemandOneAtATime() {
        Rig rig = new Rig(2, 16, 0);
        rig.outer.emit(0L);
        rig.outer.emit(1L);
        for (long i = 0; i < 3; i++) {
            rig.inners.get(0).emit(i);
            rig.inners.get(1).emit(100 + i);
        }
        rig.ts.request(4);
        assertEquals(List.of(0L, 100L, 1L, 101L), rig.ts.items);
        assertFalse(rig.ts.completed);
        rig.ts.assertClean();
    }
}
