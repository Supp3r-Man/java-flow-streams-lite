package com.github.sup3rmar10.flowstreams;

import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * flatMap with bounded concurrency and bounded memory.
 *
 * <ul>
 *   <li>The outer source is asked for {@code maxConcurrency} items up front and for one more each time an inner
 *       publisher finishes and is drained, so at most {@code maxConcurrency} inners are subscribed.</li>
 *   <li>Each inner is asked for {@code prefetch} items and topped up as its items are consumed, so each inner
 *       queue holds at most {@code prefetch} items.</li>
 *   <li>All downstream signals come from one drain loop guarded by a work-in-progress counter, so they are
 *       serialized and bounded by downstream demand.</li>
 *   <li>Errors are fail-fast: the first error (outer, inner, mapper or an invalid request) cancels everything,
 *       drops queued items and is signalled once.</li>
 * </ul>
 */
final class FlatMapOp<T, R> extends Source<R> {
    private final Source<T> source;
    private final Function<? super T, ? extends Flow.Publisher<? extends R>> mapper;
    private final int maxConcurrency;
    private final int prefetch;

    FlatMapOp(Source<T> source, Function<? super T, ? extends Flow.Publisher<? extends R>> mapper,
              int maxConcurrency, int prefetch) {
        this.source = source;
        this.mapper = mapper;
        this.maxConcurrency = maxConcurrency;
        this.prefetch = prefetch;
    }

    @Override
    protected void subscribeActual(Flow.Subscriber<? super R> down) {
        source.subscribe(new Outer<T, R>(down, mapper, maxConcurrency, prefetch));
    }

    private static final class Outer<T, R> implements Flow.Subscriber<T>, Flow.Subscription {
        private final Flow.Subscriber<? super R> down;
        private final Function<? super T, ? extends Flow.Publisher<? extends R>> mapper;
        private final int maxConcurrency;
        private final int prefetch;
        private final AtomicLong requested = new AtomicLong();
        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final List<Inner<R>> inners = new CopyOnWriteArrayList<>();
        private volatile Flow.Subscription up;
        private volatile boolean cancelled;
        private volatile boolean outerDone;

        Outer(Flow.Subscriber<? super R> down, Function<? super T, ? extends Flow.Publisher<? extends R>> mapper,
              int maxConcurrency, int prefetch) {
            this.down = down;
            this.mapper = mapper;
            this.maxConcurrency = maxConcurrency;
            this.prefetch = prefetch;
        }

        // ------------------------------------------------------------ outer subscriber side

        @Override
        public void onSubscribe(Flow.Subscription s) {
            Objects.requireNonNull(s, "subscription");
            if (up != null) {
                s.cancel();
                return;
            }
            up = s;
            down.onSubscribe(this);
            s.request(maxConcurrency);
        }

        @Override
        public void onNext(T t) {
            if (cancelled || outerDone) {
                return;
            }
            Flow.Publisher<? extends R> p;
            try {
                p = Objects.requireNonNull(mapper.apply(t), "the mapper returned a null Publisher");
            } catch (Throwable e) {
                fail(e);
                return;
            }
            Inner<R> inner = new Inner<>(this, prefetch);
            inners.add(inner);
            p.subscribe(inner);
        }

        @Override
        public void onError(Throwable e) {
            fail(e);
        }

        @Override
        public void onComplete() {
            outerDone = true;
            drain();
        }

        // ------------------------------------------------------------ subscription side (downstream)

        @Override
        public void request(long n) {
            if (n <= 0) {
                fail(Demand.invalidRequest(n));
                return;
            }
            Demand.addCap(requested, n);
            drain();
        }

        @Override
        public void cancel() {
            cancelled = true;
            up.cancel();
            cancelAll();
            drain(); // lets the drain loop (if idle) clear the queues
        }

        // ------------------------------------------------------------ internals

        void fail(Throwable e) {
            if (error.compareAndSet(null, e)) {
                drain();
            }
        }

        private void cancelAll() {
            for (Inner<R> in : inners) {
                in.cancel();
            }
        }

        private void clearAll() {
            for (Inner<R> in : inners) {
                in.queue.clear();
            }
            inners.clear();
        }

        void drain() {
            if (wip.getAndIncrement() != 0) {
                return;
            }
            int missed = 1;
            for (;;) {
                if (cancelled) {
                    clearAll();
                    return; // wip stays > 0 on purpose: nothing may ever run again
                }
                Throwable ex = error.get();
                if (ex != null) {
                    cancelled = true;
                    up.cancel();
                    cancelAll();
                    clearAll();
                    down.onError(ex);
                    return;
                }
                if (outerDone && inners.isEmpty()) {
                    cancelled = true;
                    down.onComplete();
                    return;
                }
                long r = requested.get();
                long e = 0;
                boolean progress;
                do {
                    progress = false;
                    for (Inner<R> in : inners) {
                        if (e == r || cancelled || error.get() != null) {
                            break;
                        }
                        R v = in.queue.poll();
                        if (v != null) {
                            down.onNext(v);
                            e++;
                            progress = true;
                            in.consumedOne();
                        }
                    }
                } while (progress && e != r && !cancelled && error.get() == null);
                if (e != 0 && r != Long.MAX_VALUE) {
                    requested.addAndGet(-e);
                }
                boolean reaped = false;
                for (Inner<R> in : inners) {
                    boolean d = in.done; // read 'done' before checking emptiness
                    if (d && in.queue.isEmpty() && inners.remove(in)) {
                        reaped = true;
                        up.request(1); // a concurrency slot is free: pull the next outer item
                    }
                }
                if (reaped) {
                    continue;
                }
                int m = wip.addAndGet(-missed);
                if (m == 0) {
                    return;
                }
                missed = m;
            }
        }
    }

    private static final class Inner<R> implements Flow.Subscriber<R> {
        private final Outer<?, R> parent;
        private final int prefetch;
        private final int limit;
        final Queue<R> queue = new ConcurrentLinkedQueue<>();
        private volatile Flow.Subscription s;
        volatile boolean done;
        private int consumed; // only touched inside the drain loop

        Inner(Outer<?, R> parent, int prefetch) {
            this.parent = parent;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
        }

        @Override
        public void onSubscribe(Flow.Subscription sub) {
            Objects.requireNonNull(sub, "subscription");
            if (s != null) {
                sub.cancel();
                return;
            }
            s = sub;
            if (parent.cancelled) { // cancelAll() may have run before s was set
                sub.cancel();
                return;
            }
            sub.request(prefetch);
        }

        @Override
        public void onNext(R item) {
            if (done) {
                return;
            }
            if (item == null) {
                cancel();
                parent.fail(new NullPointerException("an inner publisher emitted null"));
                return;
            }
            queue.offer(item);
            parent.drain();
        }

        @Override
        public void onError(Throwable e) {
            parent.fail(e);
        }

        @Override
        public void onComplete() {
            done = true;
            parent.drain();
        }

        void cancel() {
            Flow.Subscription x = s;
            if (x != null) {
                x.cancel();
            }
        }

        void consumedOne() {
            if (++consumed == limit) {
                consumed = 0;
                s.request(limit);
            }
        }
    }
}
