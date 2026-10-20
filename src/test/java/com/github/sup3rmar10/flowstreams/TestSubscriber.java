package com.github.sup3rmar10.flowstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * A subscriber that records everything and checks the subscriber-visible Reactive Streams rules while it does so.
 * It never throws from a signal method; violations are collected and asserted by {@link #assertClean()}.
 *
 * <p>Checked: onSubscribe first and once (1.9), never more onNext than requested (1.1), no onNext nested in an
 * onNext on the same thread (3.3 / 1.3), no signals from two threads at once (1.3), nothing after a terminal
 * signal (1.7), exactly one terminal signal (1.7), no null items (2.13), and (optionally) nothing after cancel()
 * has returned (1.8; only meaningful for single-threaded tests, because 1.8 only demands "eventually").
 */
final class TestSubscriber<T> implements Flow.Subscriber<T> {
    final List<T> items = Collections.synchronizedList(new ArrayList<>());
    final List<String> violations = new CopyOnWriteArrayList<>();
    final AtomicInteger subscribeCount = new AtomicInteger();
    final AtomicInteger terminalCount = new AtomicInteger();
    final CountDownLatch terminal = new CountDownLatch(1);
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicReference<Thread> owner = new AtomicReference<>();
    private final AtomicInteger nextDepth = new AtomicInteger();
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong totalRequested = new AtomicLong();
    private final long initialRequest;
    volatile Flow.Subscription subscription;
    volatile Throwable error;
    volatile boolean completed;
    volatile boolean cancelledByUs;
    volatile boolean strictAfterCancel = true;
    private volatile Consumer<T> onNextHook = x -> { };

    TestSubscriber() {
        this(0);
    }

    TestSubscriber(long initialRequest) {
        this.initialRequest = initialRequest;
    }

    TestSubscriber<T> hook(Consumer<T> hook) {
        this.onNextHook = hook;
        return this;
    }

    TestSubscriber<T> lenientAfterCancel() {
        this.strictAfterCancel = false;
        return this;
    }

    void request(long n) {
        if (n > 0) {
            totalRequested.accumulateAndGet(n, Demand::add); // accounted BEFORE the call: emission may be synchronous
        }
        subscription.request(n);
    }

    void cancel() {
        subscription.cancel();
        cancelledByUs = true;
    }

    boolean awaitTerminal() throws InterruptedException {
        return terminal.await(10, TimeUnit.SECONDS);
    }

    void assertClean() {
        assertEquals(List.of(), violations, "Reactive Streams rule violations");
    }

    private void enter(String signal) {
        Thread me = Thread.currentThread();
        int n = inFlight.incrementAndGet();
        if (n == 1) {
            owner.set(me);
        } else if (owner.get() != me) {
            violations.add(signal + " overlapped with another signal on a different thread (1.3)");
        }
        if (subscribeCount.get() == 0 && !signal.equals("onSubscribe")) {
            violations.add(signal + " before onSubscribe (1.9)");
        }
        if (terminalCount.get() > 0) {
            violations.add(signal + " after a terminal signal (1.7)");
        }
        if (strictAfterCancel && cancelledByUs) {
            violations.add(signal + " after cancel() returned (1.8)");
        }
    }

    private void exit() {
        inFlight.decrementAndGet();
    }

    @Override
    public void onSubscribe(Flow.Subscription s) {
        enter("onSubscribe");
        if (subscribeCount.incrementAndGet() > 1) {
            violations.add("second onSubscribe (2.5 / 1.9)");
        }
        subscription = s;
        if (initialRequest > 0) {
            request(initialRequest);
        }
        exit();
    }

    @Override
    public void onNext(T t) {
        enter("onNext");
        if (nextDepth.incrementAndGet() > 1) {
            violations.add("onNext nested inside onNext (3.3 / 1.3)");
        }
        if (t == null) {
            violations.add("null item (2.13)");
        }
        long d = delivered.incrementAndGet();
        if (d > totalRequested.get()) {
            violations.add("onNext #" + d + " but only " + totalRequested.get() + " requested (1.1)");
        }
        items.add(t);
        onNextHook.accept(t);
        nextDepth.decrementAndGet();
        exit();
    }

    @Override
    public void onError(Throwable t) {
        enter("onError");
        if (t == null) {
            violations.add("null error (2.13)");
        }
        error = t;
        terminalCount.incrementAndGet();
        terminal.countDown();
        exit();
    }

    @Override
    public void onComplete() {
        enter("onComplete");
        completed = true;
        terminalCount.incrementAndGet();
        terminal.countDown();
        exit();
    }
}
