package com.github.sup3rmar10.flowstreams;

import java.util.concurrent.Executor;
import java.util.concurrent.Flow;

/** Small helpers shared by the sources and operators. */
final class Subscriptions {
    private Subscriptions() {
    }

    /** A subscription that does nothing: used for sources that terminate immediately. */
    static final Flow.Subscription NOOP = new Flow.Subscription() {
        @Override
        public void request(long n) {
        }

        @Override
        public void cancel() {
        }
    };

    /** Rule 1.9: onSubscribe must come first, even when the source terminates straight away. */
    static void complete(Flow.Subscriber<?> s) {
        s.onSubscribe(NOOP);
        s.onComplete();
    }

    static void error(Flow.Subscriber<?> s, Throwable t) {
        s.onSubscribe(NOOP);
        s.onError(t);
    }

    /** An executor that runs the task on the calling thread (useful for tests and the demo). */
    static final Executor DIRECT = Runnable::run;
}
