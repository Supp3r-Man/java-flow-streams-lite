package com.github.sup3rmar10.flowstreams;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * publishOn: a bounded asynchronous boundary. Upstream signals are queued in an ArrayBlockingQueue of
 * {@code capacity} slots and delivered to downstream by a drain task running on the executor.
 *
 * <p>Memory is bounded by protocol, not by hope: upstream is asked for {@code capacity} items up front and is
 * asked for {@code limit} more only after {@code limit} items have been handed to downstream, so
 * (queued + outstanding upstream demand) never exceeds {@code capacity}. If an upstream ignores that and
 * overflows the queue anyway, the stream fails with an IllegalStateException instead of growing.
 *
 * <p>An upstream error is delivered after the items that were already queued (order is preserved); an invalid
 * request, a queue overflow or a rejected execution is delivered immediately and drops queued items.
 */
final class PublishOnOp<T> extends Source<T> {
    private final Source<T> source;
    private final Executor executor;
    private final int capacity;

    PublishOnOp(Source<T> source, Executor executor, int capacity) {
        this.source = source;
        this.executor = executor;
        this.capacity = capacity;
    }

    @Override
    protected void subscribeActual(Flow.Subscriber<? super T> down) {
        source.subscribe(new Sub<T>(down, executor, capacity));
    }

    private static final class Sub<T> implements Flow.Subscriber<T>, Flow.Subscription, Runnable {
        private final Flow.Subscriber<? super T> down;
        private final Executor executor;
        private final int capacity;
        private final int limit;
        private final ArrayBlockingQueue<T> queue;
        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicLong requested = new AtomicLong();
        private final AtomicReference<Throwable> failFast = new AtomicReference<>();
        private volatile Flow.Subscription up;
        private volatile boolean cancelled;
        private volatile boolean done;
        private volatile Throwable error;
        private int consumed; // drain task only

        Sub(Flow.Subscriber<? super T> down, Executor executor, int capacity) {
            this.down = down;
            this.executor = executor;
            this.capacity = capacity;
            this.limit = capacity - (capacity >> 2);
            this.queue = new ArrayBlockingQueue<>(capacity);
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            Objects.requireNonNull(s, "subscription");
            if (up != null) {
                s.cancel();
                return;
            }
            up = s;
            down.onSubscribe(this);
            s.request(capacity);
        }

        @Override
        public void onNext(T t) {
            if (done || cancelled) {
                return;
            }
            if (!queue.offer(t)) {
                up.cancel();
                fail(new IllegalStateException("publishOn queue overflow: upstream emitted more than requested"));
                return;
            }
            schedule();
        }

        @Override
        public void onError(Throwable e) {
            if (done || cancelled) {
                return;
            }
            error = e;
            done = true;
            schedule();
        }

        @Override
        public void onComplete() {
            if (done || cancelled) {
                return;
            }
            done = true;
            schedule();
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                fail(Demand.invalidRequest(n));
                return;
            }
            Demand.addCap(requested, n);
            schedule();
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                up.cancel();
                schedule(); // lets the drain task clear the queue
            }
        }

        private void fail(Throwable e) {
            if (failFast.compareAndSet(null, e)) {
                up.cancel();
                schedule();
            }
        }

        private void schedule() {
            if (wip.getAndIncrement() == 0) {
                try {
                    executor.execute(this);
                } catch (RejectedExecutionException ex) {
                    // We own the drain right (wip went 0 -> 1) and no task will ever run, so signalling here is serialized.
                    boolean wasCancelled = cancelled;
                    cancelled = true;
                    up.cancel();
                    queue.clear();
                    if (!wasCancelled) {
                        down.onError(ex);
                    }
                }
            }
        }

        @Override
        public void run() {
            int missed = 1;
            for (;;) {
                if (cancelled) {
                    queue.clear();
                    return; // wip stays > 0: terminal
                }
                Throwable ff = failFast.get();
                if (ff != null) {
                    cancelled = true;
                    queue.clear();
                    down.onError(ff);
                    return;
                }
                long r = requested.get();
                long e = 0;
                while (e != r) {
                    if (cancelled || failFast.get() != null) {
                        break;
                    }
                    boolean d = done; // read 'done' before polling
                    T v = queue.poll();
                    if (d && v == null) {
                        terminate();
                        return;
                    }
                    if (v == null) {
                        break;
                    }
                    down.onNext(v);
                    e++;
                    if (++consumed == limit) {
                        consumed = 0;
                        up.request(limit);
                    }
                }
                if (e == r && !cancelled && failFast.get() == null) {
                    boolean d = done;
                    if (d && queue.isEmpty()) { // completion needs no demand
                        terminate();
                        return;
                    }
                }
                if (e != 0 && r != Long.MAX_VALUE) {
                    requested.addAndGet(-e);
                }
                int m = wip.addAndGet(-missed);
                if (m == 0) {
                    return;
                }
                missed = m;
            }
        }

        private void terminate() {
            cancelled = true;
            Throwable ex = error;
            if (ex != null) {
                down.onError(ex);
            } else {
                down.onComplete();
            }
        }
    }
}
