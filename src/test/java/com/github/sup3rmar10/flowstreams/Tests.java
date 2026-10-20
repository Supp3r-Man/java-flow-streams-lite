package com.github.sup3rmar10.flowstreams;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

/** Small shared helpers for the tests. */
final class Tests {
    private Tests() {
    }

    static <T> TestSubscriber<T> run(Source<T> source, long request) {
        TestSubscriber<T> ts = new TestSubscriber<>(request);
        source.subscribe(ts);
        return ts;
    }

    /** Subscribes with unbounded demand, asserts the stream completed cleanly and returns the items. */
    static <T> List<T> collect(Source<T> source) {
        TestSubscriber<T> ts = run(source, Long.MAX_VALUE);
        assertTrue(ts.completed, "expected completion but got error " + ts.error);
        ts.assertClean();
        return new ArrayList<>(ts.items);
    }

    static List<Long> longs(long from, long count) {
        List<Long> out = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            out.add(from + i);
        }
        return out;
    }
}
