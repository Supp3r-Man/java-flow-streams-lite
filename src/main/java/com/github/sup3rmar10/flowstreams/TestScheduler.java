package com.github.sup3rmar10.flowstreams;

import java.time.Duration;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

/**
 * Deterministic scheduler with a virtual clock. Nothing runs until {@link #advanceBy} moves time forward; tasks
 * that become due run on the calling thread, in due-time order (first scheduled first among equal times).
 * No sleeping, no threads.
 */
public final class TestScheduler implements Scheduler {

    private static final class Task implements Handle, Comparable<Task> {
        final long due;
        final long seq;
        final Runnable body;
        boolean cancelled;

        Task(long due, long seq, Runnable body) {
            this.due = due;
            this.seq = seq;
            this.body = body;
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        @Override
        public int compareTo(Task o) {
            int c = Long.compare(due, o.due);
            return c != 0 ? c : Long.compare(seq, o.seq);
        }
    }

    private final PriorityQueue<Task> queue = new PriorityQueue<>();
    private long now;
    private long seq;

    @Override
    public synchronized long nanoTime() {
        return now;
    }

    @Override
    public synchronized Handle schedule(Runnable task, long delay, TimeUnit unit) {
        long d = Math.max(0, unit.toNanos(delay));
        Task t = new Task(now + d, seq++, task);
        queue.add(t);
        return t;
    }

    public void advanceBy(Duration d) {
        advanceBy(d.toNanos(), TimeUnit.NANOSECONDS);
    }

    public void advanceBy(long amount, TimeUnit unit) {
        long target;
        synchronized (this) {
            target = now + unit.toNanos(amount);
        }
        for (;;) {
            Task next;
            synchronized (this) {
                next = queue.peek();
                if (next == null || next.due > target) {
                    now = target;
                    return;
                }
                queue.poll();
                if (next.cancelled) {
                    continue;
                }
                now = next.due;
            }
            next.body.run();
        }
    }

    /** Number of scheduled tasks that have neither run nor been cancelled. */
    public synchronized int pendingTasks() {
        int n = 0;
        for (Task t : queue) {
            if (!t.cancelled) {
                n++;
            }
        }
        return n;
    }
}
