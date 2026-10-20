package com.github.sup3rmar10.flowstreams;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs tasks one at a time and never concurrently, on whichever thread got there first (a "serializer" or
 * mailbox). Tasks submitted while another task is running are queued and run by the thread that is already
 * inside, in submission order. Tasks that are submitted from inside a task are queued, so there is no recursion.
 * Used where signals arrive from more than one thread (the timeout operator: upstream thread plus timer thread).
 */
final class Serializer {
    private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger wip = new AtomicInteger();

    void execute(Runnable task) {
        queue.add(task);
        if (wip.getAndIncrement() != 0) {
            return;
        }
        do {
            Runnable r = queue.poll();
            try {
                r.run();
            } catch (Throwable t) {
                // Subscriber methods must not throw (rule 2.13). Keep the pipeline consistent anyway and
                // report the problem instead of losing the remaining queued tasks.
                Thread th = Thread.currentThread();
                th.getUncaughtExceptionHandler().uncaughtException(th, t);
            }
        } while (wip.decrementAndGet() != 0);
    }
}
