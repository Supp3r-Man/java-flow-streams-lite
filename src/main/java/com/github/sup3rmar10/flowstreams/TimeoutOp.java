package com.github.sup3rmar10.flowstreams;

import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * timeout: fails with a TimeoutException when no item arrives within the timeout WHILE downstream demand is
 * outstanding. With no demand the upstream is not allowed to emit, so waiting is not the upstream's fault and the
 * timer is not running. The timer is armed by a request that finds the pipeline idle, and re-armed after each
 * item if demand is still outstanding.
 *
 * <p>Signals come from two threads (upstream and the scheduler), so ALL state changes run inside a
 * {@link Serializer}; the operator's fields are only touched there. A stale timer that fires after an item has
 * already been processed is recognised by its generation number and ignored.
 */
final class TimeoutOp<T> extends Source<T> {
    private final Source<T> source;
    private final long nanos;
    private final Scheduler scheduler;

    TimeoutOp(Source<T> source, long nanos, Scheduler scheduler) {
        this.source = source;
        this.nanos = nanos;
        this.scheduler = scheduler;
    }

    @Override
    protected void subscribeActual(Flow.Subscriber<? super T> down) {
        source.subscribe(new Sub<T>(down, nanos, scheduler));
    }

    private static final class Sub<T> implements Flow.Subscriber<T>, Flow.Subscription {
        private final Flow.Subscriber<? super T> down;
        private final long nanos;
        private final Scheduler scheduler;
        private final Serializer serial = new Serializer();
        private volatile Flow.Subscription up;
        private volatile boolean cancelled;

        // confined to 'serial'
        private long outstanding;
        private long generation;
        private Scheduler.Handle timer;
        private long armedAt;
        private boolean done;

        Sub(Flow.Subscriber<? super T> down, long nanos, Scheduler scheduler) {
            this.down = down;
            this.nanos = nanos;
            this.scheduler = scheduler;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            Objects.requireNonNull(s, "subscription");
            serial.execute(() -> {
                if (up != null) {
                    s.cancel();
                    return;
                }
                up = s;
                down.onSubscribe(this);
            });
        }

        @Override
        public void onNext(T t) {
            serial.execute(() -> {
                if (done || cancelled) {
                    return;
                }
                generation++;
                disarm();
                if (outstanding != Long.MAX_VALUE) {
                    outstanding--;
                }
                down.onNext(t);
                if (!done && !cancelled && outstanding > 0) {
                    arm();
                }
            });
        }

        @Override
        public void onError(Throwable e) {
            serial.execute(() -> {
                if (done || cancelled) {
                    return;
                }
                done = true;
                disarm();
                down.onError(e);
            });
        }

        @Override
        public void onComplete() {
            serial.execute(() -> {
                if (done || cancelled) {
                    return;
                }
                done = true;
                disarm();
                down.onComplete();
            });
        }

        @Override
        public void request(long n) {
            serial.execute(() -> {
                if (done || cancelled) {
                    return;
                }
                if (n <= 0) {
                    up.request(n); // the source reports the 3.9 error through its own serialized path
                    return;
                }
                outstanding = Demand.add(outstanding, n);
                if (timer == null) {
                    arm();
                }
                up.request(n);
            });
        }

        @Override
        public void cancel() {
            cancelled = true;
            Flow.Subscription s = up;
            if (s != null) {
                s.cancel();
            }
            serial.execute(this::disarm);
        }

        private void arm() {
            long g = generation;
            armedAt = scheduler.nanoTime();
            timer = scheduler.schedule(() -> serial.execute(() -> onTimeout(g)), nanos, TimeUnit.NANOSECONDS);
        }

        private void disarm() {
            if (timer != null) {
                timer.cancel();
                timer = null;
            }
        }

        private void onTimeout(long g) {
            if (done || cancelled || g != generation) {
                return;
            }
            done = true;
            timer = null;
            long waitedMs = TimeUnit.NANOSECONDS.toMillis(scheduler.nanoTime() - armedAt);
            up.cancel();
            down.onError(new TimeoutException("no item within " + TimeUnit.NANOSECONDS.toMillis(nanos)
                    + " ms (waited " + waitedMs + " ms)"));
        }
    }
}
