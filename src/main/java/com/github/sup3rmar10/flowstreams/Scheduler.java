package com.github.sup3rmar10.flowstreams;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The only notion of time the library uses. Operators never call {@code System.nanoTime()} or sleep; they are
 * handed a Scheduler, so tests can inject {@link TestScheduler} and move a virtual clock by hand.
 */
public interface Scheduler {

    /** Current time in nanoseconds on this scheduler's clock (only differences are meaningful). */
    long nanoTime();

    /** Runs {@code task} once after {@code delay}. The returned handle cancels it if it has not run yet. */
    Handle schedule(Runnable task, long delay, TimeUnit unit);

    interface Handle {
        void cancel();
    }

    /** A real-time scheduler backed by a ScheduledExecutorService. The caller owns the executor's lifecycle. */
    static Scheduler from(ScheduledExecutorService executor) {
        return new Scheduler() {
            @Override
            public long nanoTime() {
                return System.nanoTime();
            }

            @Override
            public Handle schedule(Runnable task, long delay, TimeUnit unit) {
                ScheduledFuture<?> f = executor.schedule(task, delay, unit);
                return () -> f.cancel(false);
            }
        };
    }
}
