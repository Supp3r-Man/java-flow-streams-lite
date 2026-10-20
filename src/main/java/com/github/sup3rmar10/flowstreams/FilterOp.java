package com.github.sup3rmar10.flowstreams;

import java.util.concurrent.Flow;
import java.util.function.Predicate;

final class FilterOp<T> extends Source<T> {
    private final Source<T> source;
    private final Predicate<? super T> predicate;

    FilterOp(Source<T> source, Predicate<? super T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    protected void subscribeActual(Flow.Subscriber<? super T> down) {
        source.subscribe(new Sub<T>(down, predicate));
    }

    private static final class Sub<T> extends Link<T, T> {
        private final Predicate<? super T> predicate;

        Sub(Flow.Subscriber<? super T> down, Predicate<? super T> predicate) {
            super(down);
            this.predicate = predicate;
        }

        @Override
        protected void next(T t) {
            boolean keep;
            try {
                keep = predicate.test(t);
            } catch (Throwable e) {
                fail(e);
                return;
            }
            if (keep) {
                down.onNext(t);
            } else {
                // The dropped item consumed one unit of downstream demand; ask upstream for a replacement.
                up.request(1);
            }
        }
    }
}
