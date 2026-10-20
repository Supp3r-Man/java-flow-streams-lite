package com.github.sup3rmar10.flowstreams;

import java.util.concurrent.Flow;

/**
 * Base class for the one-in-one-out operators (map, filter, take, buffer): a Subscriber of the upstream and the
 * Subscription handed to the downstream. It enforces the common rules in one place:
 * a second onSubscribe is cancelled (2.5), nothing is forwarded after a terminal signal or after cancel (1.7, 1.8),
 * and upstream signals are serialized by the upstream (1.3), so plain fields are safe.
 */
abstract class Link<T, R> implements Flow.Subscriber<T>, Flow.Subscription {
    protected final Flow.Subscriber<? super R> down;
    protected volatile Flow.Subscription up;
    protected boolean done;
    protected volatile boolean cancelled;

    Link(Flow.Subscriber<? super R> down) {
        this.down = down;
    }

    @Override
    public final void onSubscribe(Flow.Subscription s) {
        if (s == null) {
            throw new NullPointerException("subscription");
        }
        if (up != null) {
            s.cancel();
            return;
        }
        up = s;
        down.onSubscribe(this);
    }

    @Override
    public final void onNext(T t) {
        if (done || cancelled) {
            return;
        }
        next(t);
    }

    @Override
    public final void onError(Throwable e) {
        if (done || cancelled) {
            return;
        }
        done = true;
        down.onError(e);
    }

    @Override
    public final void onComplete() {
        if (done || cancelled) {
            return;
        }
        complete();
    }

    protected abstract void next(T t);

    protected void complete() {
        done = true;
        down.onComplete();
    }

    /** An operator-level failure (mapper threw, ...): stop upstream and signal the error once. */
    protected final void fail(Throwable e) {
        done = true;
        up.cancel();
        down.onError(e);
    }

    @Override
    public void request(long n) {
        up.request(n);
    }

    @Override
    public void cancel() {
        cancelled = true;
        up.cancel();
    }
}
