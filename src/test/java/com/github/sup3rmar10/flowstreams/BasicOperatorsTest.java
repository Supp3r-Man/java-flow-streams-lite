package com.github.sup3rmar10.flowstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Behaviour of the sources and of map, filter, take and buffer, beyond the generic rule checks. */
class BasicOperatorsTest {
    static final RuntimeException BOOM = new RuntimeException("boom");

    /** An endless source of 0, 1, 2, ... that counts how many elements were actually pulled. */
    static final class Counting implements Iterable<Long> {
        final AtomicLong pulled = new AtomicLong();

        @Override
        public Iterator<Long> iterator() {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return true;
                }

                @Override
                public Long next() {
                    return pulled.getAndIncrement();
                }
            };
        }
    }

    // ------------------------------------------------------------------ factories

    @Test
    void justAndRangeEmitInOrder() {
        assertEquals(List.of("a", "b", "c"), Tests.collect(Source.just("a", "b", "c")));
        assertEquals(List.of(5L, 6L, 7L), Tests.collect(Source.range(5, 3)));
        assertEquals(List.of(-2L, -1L, 0L), Tests.collect(Source.range(-2, 3)));
        assertEquals(List.of(), Tests.collect(Source.range(0, 0)));
        assertEquals(List.of(Long.MAX_VALUE - 1, Long.MAX_VALUE), Tests.collect(Source.range(Long.MAX_VALUE - 1, 2)));
    }

    @Test
    void rangeRejectsNegativeCountAndOverflow() {
        assertThrows(IllegalArgumentException.class, () -> Source.range(0, -1));
        assertThrows(IllegalArgumentException.class, () -> Source.range(Long.MAX_VALUE, 2));
    }

    @Test
    void emptyErrorAndNeverBehaveAsNamed() {
        TestSubscriber<Object> e = Tests.run(Source.empty(), 0);
        assertTrue(e.completed);
        TestSubscriber<Object> err = Tests.run(Source.error(BOOM), 0);
        assertSame(BOOM, err.error);
        TestSubscriber<Object> never = Tests.run(Source.never(), Long.MAX_VALUE);
        assertEquals(0, never.terminalCount.get());
        assertEquals(1, never.subscribeCount.get());
        never.assertClean();
    }

    @Test
    void subscribingTwiceStartsAFreshRunEachTime() {
        Source<Long> s = Source.range(0, 3);
        assertEquals(Tests.collect(s), Tests.collect(s));
        List<Integer> data = List.of(1, 2);
        assertEquals(List.of(1, 2), Tests.collect(Source.fromIterable(data)));
        assertEquals(List.of(1, 2), Tests.collect(Source.fromIterable(data)));
    }

    @Test
    void nothingIsPulledFromTheIterableBeforeRequestAndExactlyRequestedAfter() {
        Counting c = new Counting();
        TestSubscriber<Long> ts = Tests.run(Source.fromIterable(c), 0);
        assertEquals(0, c.pulled.get());
        ts.request(2);
        assertEquals(2, c.pulled.get());
        ts.request(3);
        assertEquals(5, c.pulled.get());
        ts.cancel();
        ts.request(10);
        assertEquals(5, c.pulled.get());
        ts.assertClean();
    }

    @Test
    void iteratorFailuresBecomeOnError() {
        Iterable<Long> failsToCreate = () -> {
            throw BOOM;
        };
        assertSame(BOOM, Tests.run(Source.fromIterable(failsToCreate), 1).error);

        Iterable<Long> failsMidway = () -> new Iterator<>() {
            int i;

            @Override
            public boolean hasNext() {
                if (i == 2) {
                    throw BOOM;
                }
                return true;
            }

            @Override
            public Long next() {
                return (long) i++;
            }
        };
        TestSubscriber<Long> ts = Tests.run(Source.fromIterable(failsMidway), Long.MAX_VALUE);
        assertEquals(List.of(0L, 1L), ts.items);
        assertSame(BOOM, ts.error);
        ts.assertClean();
    }

    @Test
    void nullElementsAreAnErrorNotASignal() {
        TestSubscriber<String> ts = Tests.run(Source.just("a", null, "c"), Long.MAX_VALUE);
        assertEquals(List.of("a"), ts.items);
        assertInstanceOf(NullPointerException.class, ts.error);
        ts.assertClean();
    }

    @Test
    void fromAdaptsAForeignPublisherAndReusesSources() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp), 1);
        mp.emit(7L);
        assertEquals(List.of(7L), ts.items);
        Source<Long> s = Source.range(0, 1);
        assertSame(s, Source.from(s));
    }

    @Test
    void argumentsAreNullChecked() {
        assertThrows(NullPointerException.class, () -> Source.fromIterable(null));
        assertThrows(NullPointerException.class, () -> Source.error(null));
        assertThrows(NullPointerException.class, () -> Source.from(null));
        assertThrows(NullPointerException.class, () -> Source.range(0, 1).map(null));
        assertThrows(NullPointerException.class, () -> Source.range(0, 1).filter(null));
        assertThrows(NullPointerException.class, () -> Source.range(0, 1).onErrorResume(null));
    }

    @Test
    void concatWithAndMergeWithAreSugar() {
        assertEquals(List.of(1, 2, 3, 4), Tests.collect(Source.just(1, 2).concatWith(Source.just(3, 4))));
        List<Integer> merged = Tests.collect(Source.just(1, 2).mergeWith(Source.just(3, 4)));
        assertEquals(4, merged.size());
        assertTrue(merged.containsAll(List.of(1, 2, 3, 4)));
    }

    // ------------------------------------------------------------------ map

    @Test
    void mapTransformsInOrder() {
        assertEquals(List.of("0", "10", "20"), Tests.collect(Source.range(0, 3).map(x -> String.valueOf(x * 10))));
    }

    @Test
    void mapperExceptionBecomesOnErrorAndCancelsUpstream() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).map(x -> {
            throw BOOM;
        }), Long.MAX_VALUE);
        mp.emit(1L);
        assertSame(BOOM, ts.error);
        assertTrue(mp.cancelled);
        mp.emit(2L); // a late signal from upstream is dropped
        mp.complete();
        assertEquals(1, ts.terminalCount.get());
        ts.assertClean();
    }

    @Test
    void mapperReturningNullIsANullPointerError() {
        TestSubscriber<Object> ts = Tests.run(Source.range(0, 3).map(x -> null), Long.MAX_VALUE);
        assertInstanceOf(NullPointerException.class, ts.error);
        assertTrue(ts.items.isEmpty());
    }

    @Test
    void lateSignalsAfterCancelAreDroppedEvenFromAMisbehavingUpstream() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).map(x -> x), Long.MAX_VALUE);
        ts.cancel();
        mp.emit(1L);
        mp.complete();
        assertTrue(ts.items.isEmpty());
        assertEquals(0, ts.terminalCount.get());
        ts.assertClean();
    }

    @Test
    void aSecondOnSubscribeIsCancelled() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).map(x -> x), 1);
        AtomicInteger cancels = new AtomicInteger();
        mp.subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
            }

            @Override
            public void cancel() {
                cancels.incrementAndGet();
            }
        });
        assertEquals(1, cancels.get());
        assertEquals(1, ts.subscribeCount.get());
    }

    // ------------------------------------------------------------------ filter

    @Test
    void filterAsksUpstreamForOneReplacementPerRejectedItem() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).filter(x -> x % 2 == 0), 1);
        assertEquals(1, mp.requestedTotal.get());
        mp.emit(1L);
        assertEquals(2, mp.requestedTotal.get());
        mp.emit(3L);
        assertEquals(3, mp.requestedTotal.get());
        mp.emit(4L);
        assertEquals(List.of(4L), ts.items);
        assertEquals(3, mp.requestedTotal.get());
        ts.assertClean();
    }

    @Test
    void filterFindsTheOnlyMatchWithADemandOfOne() {
        TestSubscriber<Long> ts = Tests.run(Source.range(0, 100).filter(x -> x >= 50), 1);
        assertEquals(List.of(50L), ts.items);
        ts.assertClean();
    }

    @Test
    void predicateExceptionFailsTheStreamAndCancelsUpstream() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).filter(x -> {
            throw BOOM;
        }), 1);
        mp.emit(1L);
        assertSame(BOOM, ts.error);
        assertTrue(mp.cancelled);
    }

    // ------------------------------------------------------------------ take

    @Test
    void takeZeroCompletesWithoutSubscribingUpstream() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).take(0), 0);
        assertTrue(ts.completed);
        assertEquals(0, mp.subscribeCount.get());
        ts.assertClean();
    }

    @Test
    void takeCancelsUpstreamAfterTheLimitAndCompletes() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).take(2), Long.MAX_VALUE);
        mp.emit(1L);
        assertFalse(mp.cancelled);
        mp.emit(2L);
        assertTrue(mp.cancelled);
        assertTrue(ts.completed);
        assertEquals(List.of(1L, 2L), ts.items);
        ts.assertClean();
    }

    @Test
    void takeNeverPullsMoreThanTheLimitFromUpstream() {
        Counting c = new Counting();
        TestSubscriber<Long> ts = Tests.run(Source.fromIterable(c).take(3), Long.MAX_VALUE);
        assertEquals(3, c.pulled.get());
        assertEquals(List.of(0L, 1L, 2L), ts.items);
        assertTrue(ts.completed);
    }

    @Test
    void takeCapsTheTotalUpstreamDemandAcrossRequests() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<Long> ts = Tests.run(Source.from(mp).take(5), 0);
        ts.request(3);
        assertEquals(3, mp.requestedTotal.get());
        ts.request(4);
        assertEquals(5, mp.requestedTotal.get());
        ts.request(100);
        assertEquals(5, mp.requestedTotal.get());
    }

    @Test
    void takeDoesNotCompleteWhenDownstreamCancelsInsideTheLastItem() {
        TestSubscriber<Long> ts = new TestSubscriber<>(Long.MAX_VALUE);
        ts.hook(x -> {
            if (x == 1L) {
                ts.cancel();
            }
        });
        Source.range(0, 10).take(2).subscribe(ts);
        assertEquals(List.of(0L, 1L), ts.items);
        assertEquals(0, ts.terminalCount.get());
        ts.assertClean();
    }

    @Test
    void takeLongerThanUpstreamJustCompletes() {
        assertEquals(List.of(0L, 1L), Tests.collect(Source.range(0, 2).take(5)));
        assertThrows(IllegalArgumentException.class, () -> Source.range(0, 2).take(-1));
    }

    @Test
    void takePassesAnUpstreamErrorThatHappensBeforeTheLimit() {
        TestSubscriber<Long> ts = Tests.run(Source.range(0, 2).concatWith(Source.<Long>error(BOOM)).take(5), Long.MAX_VALUE);
        assertEquals(List.of(0L, 1L), ts.items);
        assertSame(BOOM, ts.error);
    }

    // ------------------------------------------------------------------ buffer

    @Test
    void bufferGroupsAndEmitsTheShortTailOnCompletion() {
        assertEquals(List.of(List.of(0L, 1L, 2L), List.of(3L, 4L, 5L), List.of(6L)),
                Tests.collect(Source.range(0, 7).buffer(3)));
        assertEquals(List.of(List.of(0L, 1L), List.of(2L, 3L)), Tests.collect(Source.range(0, 4).buffer(2)));
        assertEquals(List.of(), Tests.collect(Source.<Long>empty().buffer(3)));
    }

    @Test
    void bufferTurnsDemandOfKIntoKTimesSizeUpstream() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<List<Long>> ts = Tests.run(Source.from(mp).buffer(4), 0);
        ts.request(2);
        assertEquals(8, mp.requestedTotal.get());
        ts.request(1);
        assertEquals(12, mp.requestedTotal.get());
    }

    @Test
    void bufferDemandMultiplicationSaturates() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<List<Long>> ts = Tests.run(Source.from(mp).buffer(1000), 0);
        ts.request(Long.MAX_VALUE / 10);
        assertEquals(Long.MAX_VALUE, mp.requestedTotal.get());
    }

    @Test
    void theShortTailUsesDemandThatWasLeftOver() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<List<Long>> ts = Tests.run(Source.from(mp).buffer(3), 2);
        for (long i = 1; i <= 5; i++) {
            mp.emit(i);
        }
        mp.complete();
        assertEquals(List.of(List.of(1L, 2L, 3L), List.of(4L, 5L)), ts.items);
        assertTrue(ts.completed);
        ts.assertClean();
    }

    @Test
    void aPartialBufferIsDroppedWhenUpstreamFails() {
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<List<Long>> ts = Tests.run(Source.from(mp).buffer(3), Long.MAX_VALUE);
        mp.emit(1L);
        mp.emit(2L);
        mp.fail(BOOM);
        assertTrue(ts.items.isEmpty());
        assertSame(BOOM, ts.error);
    }

    @Test
    void bufferListsAreIndependentAndSizeIsValidated() {
        List<List<Long>> out = Tests.collect(Source.range(0, 4).buffer(2));
        assertNotSame(out.get(0), out.get(1));
        out.get(0).add(99L);
        assertEquals(List.of(2L, 3L), out.get(1));
        assertThrows(IllegalArgumentException.class, () -> Source.range(0, 1).buffer(0));
    }

    @Test
    void cancellingInsideTheShortTailPreventsTheCompletionSignal() {
        TestSubscriber<List<Long>> ts = new TestSubscriber<>(Long.MAX_VALUE);
        ts.hook(x -> ts.cancel());
        Source.range(0, 5).buffer(3).subscribe(ts);
        assertEquals(1, ts.items.size()); // cancelled inside the first full buffer
        assertEquals(0, ts.terminalCount.get());
        ManualPublisher<Long> mp = new ManualPublisher<>();
        TestSubscriber<List<Long>> ts2 = new TestSubscriber<>(Long.MAX_VALUE);
        ts2.hook(x -> ts2.cancel());
        Source.from(mp).buffer(3).subscribe(ts2);
        mp.emit(1L);
        mp.complete(); // emits the partial [1], the hook cancels, completion must be suppressed
        assertEquals(List.of(List.of(1L)), ts2.items);
        assertEquals(0, ts2.terminalCount.get());
        ts2.assertClean();
    }
}
