package com.github.sup3rmar10.flowstreams;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A publisher driven by hand from the test: the test decides when to emit, complete or fail, and can read how
 * much demand the operator under test has signalled and whether it cancelled.
 */
final class ManualPublisher<T> implements Flow.Publisher<T> {
    final AtomicInteger subscribeCount = new AtomicInteger();
    final AtomicLong requestedTotal = new AtomicLong();
    final AtomicInteger requestCalls = new AtomicInteger();
    final AtomicInteger cancelCalls = new AtomicInteger();
    final AtomicLong emitted = new AtomicLong();
    volatile Flow.Subscriber<? super T> subscriber;
    volatile boolean cancelled;

    @Override
    public void subscribe(Flow.Subscriber<? super T> s) {
        subscribeCount.incrementAndGet();
        subscriber = s;
        s.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
                requestCalls.incrementAndGet();
                if (n > 0) {
                    requestedTotal.accumulateAndGet(n, Demand::add);
                }
            }

            @Override
            public void cancel() {
                cancelled = true;
                cancelCalls.incrementAndGet();
            }
        });
    }

    /** Demand signalled but not yet used by emit(). */
    long outstanding() {
        long r = requestedTotal.get();
        return r == Long.MAX_VALUE ? Long.MAX_VALUE : r - emitted.get();
    }

    void emit(T t) {
        emitted.incrementAndGet();
        subscriber.onNext(t);
    }

    void complete() {
        subscriber.onComplete();
    }

    void fail(Throwable t) {
        subscriber.onError(t);
    }
}
