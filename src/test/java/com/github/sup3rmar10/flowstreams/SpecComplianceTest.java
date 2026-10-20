package com.github.sup3rmar10.flowstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A spec-style harness: the same Reactive Streams rules are checked against EVERY operator (and a few chains).
 * This is a hand-written subset of the rules, in the spirit of the TCK, NOT the official TCK. Rule numbers refer to
 * the Reactive Streams 1.0.x specification. Everything is single-threaded and deterministic; the async boundary
 * uses a direct executor here (real threads are exercised in ConcurrencyTest and PublishOnTest).
 */
class SpecComplianceTest {
    static final RuntimeException BOOM = new RuntimeException("boom");

    /** @param expected number of signals produced for an input of range(0, 10) */
    record Op(String name, Function<Source<Long>, Source<?>> apply, int expected, boolean propagatesErrors) {
        @Override
        public String toString() {
            return name;
        }
    }

    static Stream<Op> ops() {
        return Stream.of(
                new Op("identity", s -> s, 10, true),
                new Op("map", s -> s.map(x -> x * 2), 10, true),
                new Op("filter even", s -> s.filter(x -> x % 2 == 0), 5, true),
                new Op("filter reject all", s -> s.filter(x -> false), 0, true),
                new Op("take(7)", s -> s.take(7), 7, true),
                new Op("take(100)", s -> s.take(100), 10, true),
                new Op("buffer(3)", s -> s.buffer(3), 4, true),
                new Op("buffer(1)", s -> s.buffer(1), 10, true),
                new Op("flatMap c=2 prefetch 16", s -> s.flatMap(x -> Source.just(x, x), 2), 20, true),
                new Op("flatMap c=1 prefetch 1", s -> s.flatMap(x -> Source.just(x, x), 1, 1), 20, true),
                new Op("flatMap c=4 prefetch 2", s -> s.flatMap(x -> Source.range(x, 2), 4, 2), 20, true),
                new Op("concat(s, s)", s -> Source.concat(s, s), 20, true),
                new Op("merge(s, s)", s -> Source.merge(s, s), 20, true),
                new Op("onErrorResume, no error", s -> s.onErrorResume(e -> Source.just(-1L)), 10, false),
                new Op("onErrorResume, error at 5",
                        s -> s.map(x -> {
                            if (x == 5) {
                                throw BOOM;
                            }
                            return x;
                        }).onErrorResume(e -> Source.range(100, 3)), 8, false),
                new Op("timeout", s -> s.timeout(Duration.ofSeconds(1), new TestScheduler()), 10, true),
                new Op("publishOn(direct, 4)", s -> s.publishOn(Subscriptions.DIRECT, 4), 10, true),
                new Op("publishOn(direct, 1)", s -> s.publishOn(Subscriptions.DIRECT, 1), 10, true),
                new Op("chain map-filter-flatMap-take-buffer",
                        s -> s.map(x -> x + 1).filter(x -> x % 2 == 0).flatMap(x -> Source.just(x, x), 3)
                                .take(8).buffer(3), 3, true),
                new Op("chain publishOn-map-publishOn",
                        s -> s.publishOn(Subscriptions.DIRECT, 3).map(x -> x + 1).publishOn(Subscriptions.DIRECT, 2),
                        10, true));
    }

    private static Source<Long> input() {
        return Source.range(0, 10);
    }

    private static TestSubscriber<Object> subscribe(Source<?> s, long initial) {
        TestSubscriber<Object> ts = new TestSubscriber<>(initial);
        s.subscribe(ts);
        return ts;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void rule1_9_onSubscribeComesFirstAndOnlyOnce(Op op) {
        TestSubscriber<Object> ts = subscribe(op.apply().apply(input()), 0);
        assertEquals(1, ts.subscribeCount.get());
        assertTrue(ts.items.isEmpty(), "no item may be emitted before any request (1.1)");
        ts.assertClean();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void rule1_1_neverMoreItemsThanRequested_andNoStall(Op op) {
        TestSubscriber<Object> ts = subscribe(op.apply().apply(input()), 0);
        for (int step = 1; step <= op.expected() + 3 && ts.terminalCount.get() == 0; step++) {
            ts.request(1);
            assertEquals(Math.min(step, op.expected()), ts.items.size(), "after request #" + step);
        }
        assertTrue(ts.completed, "must complete after the last item");
        assertEquals(op.expected(), ts.items.size());
        ts.assertClean();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void rule3_17_unboundedDemandDeliversEverythingThenCompletesOnce(Op op) {
        TestSubscriber<Object> ts = subscribe(op.apply().apply(input()), Long.MAX_VALUE);
        assertEquals(op.expected(), ts.items.size());
        assertTrue(ts.completed);
        assertEquals(1, ts.terminalCount.get());
        ts.assertClean();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void rule3_17_demandThatWouldOverflowIsTreatedAsUnbounded(Op op) {
        TestSubscriber<Object> ts = subscribe(op.apply().apply(input()), 0);
        ts.request(Long.MAX_VALUE - 1);
        ts.request(10);
        assertEquals(op.expected(), ts.items.size());
        assertTrue(ts.completed);
        ts.assertClean();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void rule1_7_nothingIsSignalledAfterCompletion(Op op) {
        TestSubscriber<Object> ts = subscribe(op.apply().apply(input()), Long.MAX_VALUE);
        ts.request(5);
        ts.request(Long.MAX_VALUE);
        ts.cancel();
        assertEquals(op.expected(), ts.items.size());
        assertEquals(1, ts.terminalCount.get());
        ts.assertClean();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void rule3_5_cancelStopsSignalsAndLaterRequestsAreNoOps(Op op) {
        assumeTrue(op.expected() >= 2);
        TestSubscriber<Object> ts = subscribe(op.apply().apply(input()), 0);
        ts.request(1);
        ts.cancel();
        ts.request(Long.MAX_VALUE);
        assertEquals(1, ts.items.size());
        assertEquals(0, ts.terminalCount.get());
        ts.assertClean();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void rule3_5_3_6_cancelIsIdempotent(Op op) {
        assumeTrue(op.expected() >= 2);
        TestSubscriber<Object> ts = subscribe(op.apply().apply(input()), 2);
        ts.cancel();
        ts.cancel();
        ts.request(3);
        assertEquals(2, ts.items.size());
        ts.assertClean();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void rule1_8_cancelFromInsideOnNextStopsTheStream(Op op) {
        assumeTrue(op.expected() >= 2);
        TestSubscriber<Object> ts = new TestSubscriber<>(Long.MAX_VALUE);
        ts.hook(x -> {
            if (ts.items.size() == 2) {
                ts.cancel();
            }
        });
        op.apply().apply(input()).subscribe(ts);
        assertEquals(2, ts.items.size());
        assertEquals(0, ts.terminalCount.get(), "no terminal signal after cancel");
        ts.assertClean();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void rule3_9_requestOfZeroOrNegativeSignalsIllegalArgumentException(Op op) {
        for (long bad : new long[] {0, -1, Long.MIN_VALUE}) {
            TestSubscriber<Object> ts = subscribe(op.apply().apply(input()), 0);
            ts.request(bad);
            assertInstanceOf(IllegalArgumentException.class, ts.error, "n=" + bad);
            assertEquals(1, ts.terminalCount.get());
            assertTrue(ts.items.isEmpty());
            ts.request(5); // and the stream stays terminated
            assertEquals(1, ts.terminalCount.get());
            ts.assertClean();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void rule3_3_requestInsideOnNextDoesNotRecurseWithoutBound(Op op) {
        Source<?> pipeline = op.apply().apply(Source.range(0, 20_000));
        TestSubscriber<Object> reference = subscribe(pipeline, Long.MAX_VALUE);
        TestSubscriber<Object> ts = new TestSubscriber<>(1);
        ts.hook(x -> ts.request(1));
        pipeline.subscribe(ts); // a StackOverflowError would surface here
        assertEquals(reference.items.size(), ts.items.size());
        assertTrue(ts.completed);
        ts.assertClean();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void rule1_9_nullSubscriberIsANullPointerException(Op op) {
        Source<Object> s = cast(op.apply().apply(input()));
        assertThrows(NullPointerException.class, () -> s.subscribe(null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void rule1_9_emptyUpstreamCompletesWithoutAnyRequest(Op op) {
        TestSubscriber<Object> ts = subscribe(op.apply().apply(Source.empty()), 0);
        assertTrue(ts.completed);
        assertEquals(1, ts.terminalCount.get());
        assertTrue(ts.items.isEmpty());
        ts.assertClean();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void rule1_4_upstreamErrorIsSignalledExactlyOnce(Op op) {
        assumeTrue(op.propagatesErrors());
        Source<Long> failing = Source.range(0, 3).concatWith(Source.error(BOOM));
        TestSubscriber<Object> ts = subscribe(op.apply().apply(failing), Long.MAX_VALUE);
        assertEquals(1, ts.terminalCount.get());
        assertSame(BOOM, ts.error);
        ts.request(3);
        assertEquals(1, ts.terminalCount.get());
        ts.assertClean();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ops")
    void sameResultsAcrossIndependentSubscriptions(Op op) {
        Source<?> pipeline = op.apply().apply(input());
        TestSubscriber<Object> a = subscribe(pipeline, Long.MAX_VALUE);
        TestSubscriber<Object> b = subscribe(pipeline, Long.MAX_VALUE);
        assertEquals(a.items, b.items, "cold source: every subscription starts afresh");
        assertEquals(op.expected(), a.items.size());
    }

    @SuppressWarnings("unchecked")
    private static Source<Object> cast(Source<?> s) {
        return (Source<Object>) s;
    }

    @org.junit.jupiter.api.Test
    void harnessSelfCheck_detectsAnOverDeliveringPublisher() {
        TestSubscriber<Long> ts = new TestSubscriber<>(1);
        ManualPublisher<Long> bad = new ManualPublisher<>();
        bad.subscribe(ts);
        bad.emit(1L);
        bad.emit(2L); // 2 items against a demand of 1
        assertEquals(1, ts.violations.size());
        assertTrue(ts.violations.get(0).contains("1.1"));
        bad.complete();
        bad.emit(3L);
        assertTrue(ts.violations.stream().anyMatch(v -> v.contains("1.7")));
        assertEquals(List.of(1L, 2L, 3L), ts.items);
    }
}
