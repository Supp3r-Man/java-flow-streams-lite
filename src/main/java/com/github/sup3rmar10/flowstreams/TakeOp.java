package com.github.sup3rmar10.flowstreams;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

final class TakeOp<T> extends Source<T> {
    private final Source<T> source;
    private final long limit;

    TakeOp(Source<T> source, long limit) {
        this.source = source;
        this.limit = limit;
    }

    @Override
    protected void subscribeActual(Flow.Subscriber<? super T> down) {
        if (limit == 0) {
            // Nothing can ever be emitted: do not even subscribe upstream.
            Subscriptions.complete(down);
            return;
        }
        source.subscribe(new Sub<T>(down, limit));
    }

    private static final class Sub<T> extends Link<T, T> {
        private long remaining;
        /** How many more items we may still ask upstream for (so upstream never over-produces past the limit). */
        private final AtomicLong budget;

        Sub(Flow.Subscriber<? super T> down, long limit) {
            super(down);
            this.remaining = limit;
            this.budget = new AtomicLong(limit);
        }

        @Override
        protected void next(T t) {
            long left = --remaining;
            down.onNext(t);
            if (left == 0) {
                done = true;
                up.cancel();
                if (!cancelled) {
                    down.onComplete();
                }
            }
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                up.request(n); // let the source signal the 3.9 error in its own serialized path
                return;
            }
            for (;;) {
                long b = budget.get();
                if (b == 0) {
                    return;
                }
                long grant = Math.min(n, b);
                if (budget.compareAndSet(b, b - grant)) {
                    up.request(grant);
                    return;
                }
            }
        }
    }
}
