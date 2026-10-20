package com.github.sup3rmar10.flowstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class TestSchedulerTest {

    @Test
    void nothingRunsUntilTimeIsAdvancedAndTheClockStartsAtZero() {
        TestScheduler s = new TestScheduler();
        List<String> log = new ArrayList<>();
        s.schedule(() -> log.add("a"), 10, TimeUnit.MILLISECONDS);
        assertEquals(0, s.nanoTime());
        assertEquals(List.of(), log);
        s.advanceBy(Duration.ofMillis(9));
        assertEquals(List.of(), log);
        s.advanceBy(Duration.ofMillis(1));
        assertEquals(List.of("a"), log);
        assertEquals(10_000_000L, s.nanoTime());
    }

    @Test
    void tasksRunInDueOrderAndFirstScheduledFirstAmongEqualTimes() {
        TestScheduler s = new TestScheduler();
        List<String> log = new ArrayList<>();
        s.schedule(() -> log.add("late"), 30, TimeUnit.MILLISECONDS);
        s.schedule(() -> log.add("first-10"), 10, TimeUnit.MILLISECONDS);
        s.schedule(() -> log.add("second-10"), 10, TimeUnit.MILLISECONDS);
        s.advanceBy(Duration.ofMillis(100));
        assertEquals(List.of("first-10", "second-10", "late"), log);
    }

    @Test
    void clockShowsTheTasksDueTimeWhileItRuns() {
        TestScheduler s = new TestScheduler();
        List<Long> seen = new ArrayList<>();
        s.schedule(() -> seen.add(s.nanoTime()), 5, TimeUnit.SECONDS);
        s.schedule(() -> seen.add(s.nanoTime()), 2, TimeUnit.SECONDS);
        s.advanceBy(Duration.ofSeconds(10));
        assertEquals(List.of(2_000_000_000L, 5_000_000_000L), seen);
        assertEquals(10_000_000_000L, s.nanoTime());
    }

    @Test
    void cancelledTasksNeverRunAndAreNotPending() {
        TestScheduler s = new TestScheduler();
        List<String> log = new ArrayList<>();
        Scheduler.Handle h = s.schedule(() -> log.add("x"), 1, TimeUnit.SECONDS);
        s.schedule(() -> log.add("y"), 1, TimeUnit.SECONDS);
        assertEquals(2, s.pendingTasks());
        h.cancel();
        assertEquals(1, s.pendingTasks());
        s.advanceBy(Duration.ofSeconds(2));
        assertEquals(List.of("y"), log);
        assertEquals(0, s.pendingTasks());
    }

    @Test
    void aTaskScheduledByATaskRunsInTheSameAdvanceIfItIsDueByThen() {
        TestScheduler s = new TestScheduler();
        List<String> log = new ArrayList<>();
        s.schedule(() -> {
            log.add("outer@" + s.nanoTime());
            s.schedule(() -> log.add("inner@" + s.nanoTime()), 5, TimeUnit.NANOSECONDS);
            s.schedule(() -> log.add("too-late"), 1, TimeUnit.HOURS);
        }, 10, TimeUnit.NANOSECONDS);
        s.advanceBy(20, TimeUnit.NANOSECONDS);
        assertEquals(List.of("outer@10", "inner@15"), log);
        assertEquals(1, s.pendingTasks());
    }

    @Test
    void negativeDelayMeansRunOnTheNextAdvance() {
        TestScheduler s = new TestScheduler();
        List<String> log = new ArrayList<>();
        s.schedule(() -> log.add("now"), -5, TimeUnit.SECONDS);
        s.advanceBy(Duration.ZERO);
        assertEquals(List.of("now"), log);
    }
}
