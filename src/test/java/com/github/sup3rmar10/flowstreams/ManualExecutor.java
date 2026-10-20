package com.github.sup3rmar10.flowstreams;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

/** An executor that runs nothing until the test says so: makes async boundaries deterministic. */
final class ManualExecutor implements Executor {
    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();

    @Override
    public void execute(Runnable command) {
        tasks.add(command);
    }

    int pending() {
        return tasks.size();
    }

    /** Runs queued tasks (including ones queued while running) until none are left; returns how many ran. */
    int runAll() {
        int n = 0;
        Runnable r;
        while ((r = tasks.poll()) != null) {
            r.run();
            n++;
        }
        return n;
    }
}
