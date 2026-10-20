package com.github.sup3rmar10.flowstreams;

import java.util.Iterator;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

/** Emits the elements of an Iterable, honouring request(n). The reference implementation of the emission loop. */
final class IterableSource<T> extends Source<T> {
    private final Iterable<? extends T> iterable;

    IterableSource(Iterable<? extends T> iterable) {
        this.iterable = iterable;
    }

    @Override
    protected void subscribeActual(Flow.Subscriber<? super T> s) {
        Iterator<? extends T> it;
        boolean has;
        try {
            it = iterable.iterator();
            has = it.hasNext();
        } catch (Throwable e) {
            Subscriptions.error(s, e);
            return;
        }
        if (!has) {
            Subscriptions.complete(s);
            return;
        }
        s.onSubscribe(new IteratorSubscription<>(s, it));
    }

    static final class IteratorSubscription<T> implements Flow.Subscription {
        private final Flow.Subscriber<? super T> actual;
        private final Iterator<? extends T> it;
        /** Outstanding demand. Doubles as the ownership token of the emission loop (0 -> n means "you own it"). */
        private final AtomicLong requested = new AtomicLong();
        private volatile boolean cancelled;
        private volatile boolean invalid;
        private volatile long invalidValue;

        IteratorSubscription(Flow.Subscriber<? super T> actual, Iterator<? extends T> it) {
            this.actual = actual;
            this.it = it;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                // Rule 3.9: signal onError(IllegalArgumentException), but through the emission loop so that it
                // stays serialized with onNext. Adding 1 makes sure someone owns (or keeps) the loop.
                invalidValue = n;
                invalid = true;
                if (Demand.addCap(requested, 1) == 0) {
                    drain(1);
                }
                return;
            }
            if (Demand.addCap(requested, n) == 0) {
                drain(n);
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        private boolean fatal() {
            if (cancelled) {
                return true;
            }
            if (invalid) {
                cancelled = true;
                actual.onError(Demand.invalidRequest(invalidValue));
                return true;
            }
            return false;
        }

        private void drain(long r) {
            long e = 0;
            for (;;) {
                while (e != r) {
                    if (fatal()) {
                        return;
                    }
                    T v;
                    try {
                        v = it.next();
                    } catch (Throwable ex) {
                        cancelled = true;
                        actual.onError(ex);
                        return;
                    }
                    if (v == null) {
                        cancelled = true;
                        actual.onError(new NullPointerException("the iterator produced a null element"));
                        return;
                    }
                    actual.onNext(v);
                    if (cancelled) {
                        return;
                    }
                    boolean has;
                    try {
                        has = it.hasNext();
                    } catch (Throwable ex) {
                        cancelled = true;
                        actual.onError(ex);
                        return;
                    }
                    if (!has) {
                        cancelled = true;
                        actual.onComplete();
                        return;
                    }
                    e++;
                }
                if (fatal()) {
                    return;
                }
                r = requested.get();
                if (e == r) {
                    r = requested.addAndGet(-e);
                    if (r == 0) {
                        return;
                    }
                    e = 0;
                }
            }
        }
    }
}
