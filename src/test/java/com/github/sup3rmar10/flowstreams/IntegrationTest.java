package com.github.sup3rmar10.flowstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** Whole pipelines compared with java.util.stream, under different demand patterns. Seeded, synthetic data. */
class IntegrationTest {

    private static List<Integer> data() {
        Random rnd = new Random(42);
        List<Integer> out = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            out.add(rnd.nextInt(1000));
        }
        return out;
    }

    private static List<List<Integer>> chunks(List<Integer> in, int n) {
        List<List<Integer>> out = new ArrayList<>();
        for (int i = 0; i < in.size(); i += n) {
            out.add(new ArrayList<>(in.subList(i, Math.min(in.size(), i + n))));
        }
        return out;
    }

    private static Source<List<Integer>> pipeline(List<Integer> data) {
        return Source.fromIterable(data)
                .map(x -> x * 3)
                .filter(x -> x % 2 == 0)
                .flatMap(x -> Source.just(x, x + 1), 1)
                .take(200)
                .buffer(7);
    }

    private static List<List<Integer>> expected(List<Integer> data) {
        List<Integer> flat = data.stream().map(x -> x * 3).filter(x -> x % 2 == 0)
                .flatMap(x -> Stream.of(x, x + 1)).limit(200).collect(Collectors.toList());
        return chunks(flat, 7);
    }

    @Test
    void orderedPipelineMatchesJavaStreamsWithUnboundedDemand() {
        List<Integer> d = data();
        assertEquals(expected(d), Tests.collect(pipeline(d)));
    }

    @Test
    void orderedPipelineMatchesJavaStreamsWithRandomSmallRequests() {
        List<Integer> d = data();
        Random rnd = new Random(7);
        TestSubscriber<List<Integer>> ts = Tests.run(pipeline(d), 0);
        int guard = 0;
        while (ts.terminalCount.get() == 0 && guard++ < 10_000) {
            ts.request(1 + rnd.nextInt(5));
        }
        assertTrue(ts.completed);
        assertEquals(expected(d), new ArrayList<>(ts.items));
        ts.assertClean();
    }

    @Test
    void concurrentFlatMapWithAnAsyncBoundaryHasTheSameMultisetOfItems() {
        List<Integer> d = data();
        ManualExecutor exec = new ManualExecutor();
        Source<Integer> s = Source.fromIterable(d).flatMap(x -> Source.just(x, -x), 4).publishOn(exec, 5);
        TestSubscriber<Integer> ts = Tests.run(s, 0);
        Random rnd = new Random(11);
        int guard = 0;
        while (ts.terminalCount.get() == 0 && guard++ < 100_000) {
            ts.request(1 + rnd.nextInt(4));
            exec.runAll();
        }
        List<Integer> want = d.stream().flatMap(x -> Stream.of(x, -x)).sorted().collect(Collectors.toList());
        List<Integer> got = new ArrayList<>(ts.items);
        Collections.sort(got);
        assertEquals(want, got);
        assertTrue(ts.completed);
        ts.assertClean();
    }

    @Test
    void publishOnWithRandomInterleavingOfRequestsAndExecutorRuns() {
        ManualExecutor exec = new ManualExecutor();
        TestSubscriber<Long> ts = Tests.run(Source.range(0, 1000).publishOn(exec, 8).map(x -> x + 1), 0);
        Random rnd = new Random(3);
        int guard = 0;
        while (ts.terminalCount.get() == 0 && guard++ < 100_000) {
            if (rnd.nextBoolean()) {
                ts.request(1 + rnd.nextInt(5));
            }
            if (rnd.nextBoolean()) {
                exec.runAll();
            }
        }
        assertEquals(1000, ts.items.size());
        assertEquals(Tests.longs(1, 1000), ts.items);
        ts.assertClean();
    }

    @Test
    void errorRecoveryTimeoutAndBufferingComposeUnderTake() {
        TestScheduler clock = new TestScheduler();
        Source<List<Long>> s = Source.range(0, 100)
                .map(x -> {
                    if (x == 30) {
                        throw new IllegalStateException("synthetic failure at 30");
                    }
                    return x;
                })
                .onErrorResume(e -> Source.range(1000, 5))
                .timeout(Duration.ofSeconds(1), clock)
                .buffer(10)
                .take(4);
        List<List<Long>> out = Tests.collect(s);
        assertEquals(List.of(Tests.longs(0, 10), Tests.longs(10, 10), Tests.longs(20, 10), Tests.longs(1000, 5)), out);
        assertEquals(0, clock.pendingTasks());
    }
}
