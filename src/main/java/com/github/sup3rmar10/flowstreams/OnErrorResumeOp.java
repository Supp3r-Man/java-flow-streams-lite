package com.github.sup3rmar10.flowstreams;

import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.function.Function;

/**
 * onErrorResume: when upstream fails, subscribe to the fallback and carry the OUTSTANDING demand over to it
 * (requested so far minus delivered so far), so downstream sees one uninterrupted, correctly accounted stream.
 */
final class OnErrorResumeOp<T> extends Source<T> {
    private final Source<T> source;
    private final Function<? super Throwable, ? extends Flow.Publisher<? extends T>> fallback;

    OnErrorResumeOp(Source<T> source, Function<? super Throwable, ? extends Flow.Publisher<? extends T>> fallback) {
        this.source = source;
        this.fallback = fallback;
    }

    @Override
    protected void subscribeActual(Flow.Subscriber<? super T> down) {
        source.subscribe(new Sub<T>(down, fallback));
    }

    private static final class Sub<T> implements Flow.Subscriber<T>, Flow.Subscription {
        private final Flow.Subscriber<? super T> down;
        private final Function<? super Throwable, ? extends Flow.Publisher<? extends T>> fallback;

        // Guarded by 'lock'. The lock is never held while calling into foreign code.
        private final Object lock = new Object();
        private Flow.Subscription current;
        private long requested;
        private boolean cancelled;

        // Touched only from the (serialized) signal path.
        private boolean started;
        private boolean switched;
        private boolean done;
        private long produced;

        private volatile boolean badRequest;
        private volatile boolean cancelledFlag;

        Sub(Flow.Subscriber<? super T> down, Function<? super Throwable, ? extends Flow.Publisher<? extends T>> fallback) {
            this.down = down;
            this.fallback = fallback;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            Objects.requireNonNull(s, "subscription");
            if (started && !switched) {
                s.cancel(); // rule 2.5
                return;
            }
            long r;
            synchronized (lock) {
                if (cancelled) {
                    r = -1;
                } else {
                    current = s;
                    r = requested;
                }
            }
            if (r < 0) {
                s.cancel();
                return;
            }
            if (!started) {
                started = true;
                down.onSubscribe(this);
            } else if (r > 0) {
                s.request(r);
            }
        }

        @Override
        public void onNext(T t) {
            if (done || cancelledFlag) {
                return;
            }
            produced++;
            down.onNext(t);
        }

        @Override
        public void onError(Throwable e) {
            if (done || cancelledFlag) {
                return;
            }
            if (switched || badRequest) {
                done = true;
                down.onError(e);
                return;
            }
            Flow.Publisher<? extends T> next;
            try {
                next = Objects.requireNonNull(fallback.apply(e), "the fallback function returned null");
            } catch (Throwable x) {
                x.addSuppressed(e);
                done = true;
                down.onError(x);
                return;
            }
            switched = true;
            synchronized (lock) {
                if (requested != Long.MAX_VALUE) {
                    requested = Math.max(0, requested - produced);
                }
            }
            produced = 0;
            next.subscribe(this);
        }

        @Override
        public void onComplete() {
            if (done || cancelledFlag) {
                return;
            }
            done = true;
            down.onComplete();
        }

        @Override
        public void request(long n) {
            Flow.Subscription s;
            if (n <= 0) {
                badRequest = true; // an IllegalArgumentException from the source must reach downstream, not the fallback
                synchronized (lock) {
                    s = current;
                }
                s.request(n);
                return;
            }
            synchronized (lock) {
                if (cancelled) {
                    return;
                }
                requested = Demand.add(requested, n);
                s = current;
            }
            s.request(n);
        }

        @Override
        public void cancel() {
            Flow.Subscription s;
            synchronized (lock) {
                cancelled = true;
                cancelledFlag = true;
                s = current;
            }
            if (s != null) {
                s.cancel();
            }
        }
    }
}
