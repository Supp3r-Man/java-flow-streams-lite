# java-flow-streams-lite

A tiny reactive-streams library on top of `java.util.concurrent.Flow`, in Java 17. It exists to show how
backpressure actually works: `request(n)` accounting, serialized signals, no signals after cancel or completion,
`Long.MAX_VALUE` as "unbounded", and overflow-safe demand arithmetic. The only dependency is JUnit 5 (test scope).

**This is not a replacement for Reactor or RxJava.** It is a learning/showcase project. In particular, I did
**not** run the official Reactive Streams TCK. The tests include a hand-written, TCK-inspired rule harness
(see below), which is a subset of the spec checked by me, not a certification.

What is in it:

- `Source<T>`: a cold, Flux-like `Flow.Publisher`. Factories: `just`, `fromIterable`, `range`, `empty`, `error`,
  `never`, `from(Publisher)`, `merge(...)`, `concat(...)`.
- Operators: `map`, `filter`, `take`, `flatMap(mapper, maxConcurrency[, prefetch])`, `buffer(n)`, `mergeWith`,
  `concatWith`, `onErrorResume`, `timeout(Duration, Scheduler)`, `publishOn(Executor, capacity)`.
- `Scheduler` (the only notion of time; `Scheduler.from(ScheduledExecutorService)` for real time) and
  `TestScheduler` (virtual clock, no sleeping, no threads).
- `Demo`: a fast producer against a deliberately slow consumer, showing the producer can never run more than the
  queue capacity ahead.

## How to run

```bash
mvn -q -B verify                                                   # compile and run all tests
java -cp target/classes com.github.sup3rmar10.flowstreams.Demo     # the demo (no extra jars needed)
python scripts/mutation_check.py                                   # the mutation check, about 15 minutes
```

Clone: `git clone https://github.com/Supp3r-Man/java-flow-streams-lite.git`

```java
Source.range(0, 1_000_000)
      .map(i -> i * 2)
      .publishOn(executor, 16)          // at most 16 items ever queued
      .flatMap(i -> lookup(i), 4)       // at most 4 inner publishers at once
      .take(100)
      .subscribe(mySubscriber);         // any java.util.concurrent.Flow.Subscriber
```

## Demo output (real run)

```
== 1. fast producer, slow consumer, publishOn capacity 16
consumer handled 300 items, one request(1) at a time
producer generated 304 of 1000000 possible elements
max (produced - consumed) ever observed: 16  (hard bound = capacity = 16)
bounded memory: yes

== 2. concat, flatMap(concurrency 2), buffer(4)
concat(a, b)           = [1, 2, 3, 10, 20, 30]
range(1,6).flatMap(x -> just(x, -x), 2) = [1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6]
range(1,10).buffer(4)  = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10]]

== 3. timeout(5 s) driven by a virtual clock (no sleeping)
after 4999 ms: waiting
after 5000 ms: error: java.util.concurrent.TimeoutException: no item within 5000 ms (waited 5000 ms)
```

The consumer sleeps 1 ms per item, so the exact `produced` figure (304 here) varies by run; the bound of 16 does not.
The Demo throws if the bound is ever exceeded, and `ConcurrencyTest` asserts the same bound on 20,000 items.

## Design notes

- **Demand arithmetic** (`Demand`): additions saturate at `Long.MAX_VALUE` (rule 3.17) instead of wrapping to a negative
  number; `buffer(n)` multiplies demand by `n` with a saturating multiply.
- **Emission loops.** Sources and queue-based operators emit from a single drain loop guarded by an ownership counter
  (the requested counter itself in `IterableSource`; a work-in-progress counter in `flatMap` and `publishOn`). Whoever
  moves the counter from 0 to non-zero owns the loop; everyone else only adds demand or work and returns. That gives
  serialized signals (1.3) and bounded recursion when `request(1)` is called from inside `onNext` (3.3) without
  locks around user code.
- **One-in-one-out operators** (`map`, `filter`, `take`, `buffer`) share a small base class (`Link`) that cancels a second
  `onSubscribe` (2.5), and drops signals after a terminal signal or after `cancel()` (1.7, 1.8). `filter` asks upstream for
  one replacement per dropped item. `take(n)` never lets total upstream demand exceed `n` and cancels upstream on the
  n-th item. `take(0)` does not even subscribe upstream.
- **Invalid `request(n <= 0)`** (3.9) is forwarded to the source, which signals `IllegalArgumentException` through its own
  serialized path; operators that own a queue (`flatMap`, `publishOn`) signal it fail-fast from their drain loop.
- **`flatMap`**: asks the outer source for `maxConcurrency` items up front and for one more each time an inner finishes
  and is drained. Each inner has its own queue and is asked for `prefetch` items, topped up as items are consumed. So
  memory is bounded by `maxConcurrency * prefetch` (plus what downstream demand allows). Fail-fast: the first error
  from outer, inner, mapper or an invalid request cancels everything, drops queued items and is signalled once.
  `concat` is `flatMap` with concurrency 1; `merge` is `flatMap` with concurrency = number of sources.
- **`buffer(n)`**: downstream `request(k)` becomes upstream `request(k * n)`. A short tail is emitted on completion using
  the demand left over; a partial buffer is dropped on error.
- **`onErrorResume`**: on error, subscribes to the fallback and hands over the outstanding demand (requested minus
  delivered). A bad `request(n)` error is deliberately not resumed.
- **`timeout`**: signals come from two threads (upstream and the scheduler), so all state lives behind a small `Serializer`
  (a mailbox that runs tasks one at a time). The timer only runs while downstream demand is outstanding: a consumer that
  has not asked for anything cannot make upstream "time out". Stale timers are recognised by a generation number.
- **`publishOn`**: a bounded `ArrayBlockingQueue` of `capacity` slots drained on the executor. Upstream is asked for
  `capacity` up front and for `limit` (3/4 of capacity) more only after `limit` items were handed downstream, so
  queued + outstanding never exceeds `capacity`. An upstream that over-produces gets an `IllegalStateException`, not a
  bigger queue. An upstream error is delivered after already-queued items. A rejected execution becomes `onError`.

## Tests

`mvn -q -B verify` reports **398 test invocations** (5 skipped by JUnit assumptions, so 393 executed, 0 failing), from
132 test methods:

- `SpecComplianceTest`: 14 rule checks run against each of 20 operator pipelines (every operator plus chains), via
  `@ParameterizedTest`: 280 invocations, plus a self-check of the harness. Rules checked (numbers refer to the Reactive
  Streams 1.0.x spec): onSubscribe first and once (1.9, 2.5); never more `onNext` than requested and no stall under
  `request(1)` steps (1.1); unbounded and overflowing demand (3.17); nothing after completion (1.7); cancel stops signals,
  is idempotent, works from inside `onNext` (1.8, 3.5, 3.6); `request(<=0)` gives `IllegalArgumentException` (3.9);
  `request(1)` inside `onNext` does not overflow the stack (3.3); null subscriber is an NPE (1.9); empty upstream completes
  without demand; an upstream error arrives exactly once (1.4/1.7); cold sources restart per subscription.
  `TestSubscriber` is the checker: it records violations (overlapping signals from different threads, nested `onNext`,
  over-delivery, signals after terminal or after cancel) instead of throwing.
- Operator tests with a hand-driven `ManualPublisher` (inspect exactly how much demand was signalled, whether it was
  cancelled) for `map`, `filter`, `take`, `buffer`, `flatMap`/`merge`/`concat`, `onErrorResume`, `timeout`, `publishOn`.
- `TestSchedulerTest`, `DemandTest` (including 8 threads adding demand concurrently).
- `ConcurrencyTest`: real threads, asserting invariants only (serial signals, exactly-once, in-order, never more than
  requested, bounded run-ahead). It waits on latches and futures; there are no `Thread.sleep` calls in the tests. The Demo
  does sleep 1 ms per item to play the "slow consumer".
- `IntegrationTest`: whole pipelines compared with `java.util.stream`, under random request sizes (seeded).

Everything else is deterministic: time comes from `TestScheduler`, the async boundary from a `ManualExecutor` that runs
tasks only when the test says so.

## Mutation check

Passing tests prove little if they would also pass on broken code, so `scripts/mutation_check.py` applies 25
behaviour-defining mutations one at a time (a textual single replacement; each match text must occur exactly once or the
mutation is reported as `BROKEN`, not as a survivor), runs `mvn test`, and restores the file in a `finally` block. A
mutant that fails to compile would be reported separately (none did). **Hangs count as kills**: the suite sets a 10 s
per-test timeout in separate-thread mode (`junit-platform.properties`), so a stalled stream fails that test instead of
hanging the build, and as a backstop a whole run over 300 s would be killed and labelled `KILLED (timeout)`. (No mutant
needed that backstop; a per-test timeout shows up as an ordinary failure in the log, so the table cannot say which kills
were hangs.) This is a hand-rolled check, not PIT, and the mutations are the ones I thought of.

**Result on the final suite: 24 of 25 killed, 1 survived (an equivalent mutant).**
The first full run, before I added three tests, was 21 of 25 killed with 4 survivors (M17, M19, M21, M25). I then
added tests for the three survivors that were real gaps and re-ran the whole script on the final suite.

| # | Mutation | Result | First failing test(s) |
|---|---|---|---|
| M01 | demand addition no longer saturates | killed | `DemandTest.additionSaturatesAtLongMaxValueInsteadOfOverflowing`, 1 more |
| M02 | demand multiplication no longer saturates | killed | `BasicOperatorsTest.bufferDemandMultiplicationSaturates`, 3 more |
| M03 | `addCap` adds without saturation | killed | `DemandTest.addCapSaturatesAndStaysUnbounded` (only this one) |
| M04 | iterable source ignores `request(n<=0)` | killed | `SpecComplianceTest.rule3_9_...`, 11 more |
| M05 | iterable source never decrements served demand | killed | 53 tests |
| M06 | second `onSubscribe` accepted instead of cancelled | killed | `BasicOperatorsTest.aSecondOnSubscribeIsCancelled` (only this one) |
| M07 | operators forward signals after done/cancel | killed | `BasicOperatorsTest.lateSignalsAfterCancelAreDroppedEvenFromAMisbehavingUpstream`, 1 more |
| M08 | filter does not ask for a replacement | killed | 17 tests |
| M09 | take forwards demand beyond its limit | killed | `BasicOperatorsTest.takeCapsTheTotalUpstreamDemandAcrossRequests` (only this one) |
| M10 | take does not cancel upstream at the limit | killed | `BasicOperatorsTest.takeCancelsUpstreamAfterTheLimitAndCompletes` (only this one) |
| M11 | buffer does not multiply demand by n | killed | 11 tests |
| M12 | buffer drops the short tail | killed | 16 tests |
| M13 | flatMap asks outer for unbounded items | killed | `FlatMapTest.aFinishedInnerFreesExactlyOneSlot`, 4 more |
| M14 | flatMap never frees a concurrency slot | killed | 63 tests |
| M15 | flatMap never tops up an inner | killed | 16 tests |
| M16 | flatMap drain ignores downstream demand | killed | 43 tests |
| M17 | flatMap inner skips the cancelled-after-subscribe check | killed (after new test) | `FlatMapTest.anInnerThatSubscribesLateAfterCancelIsCancelledImmediately` |
| M18 | onErrorResume hands over full, not outstanding, demand | killed | `ResumeTimeoutTest.demandAccountingSurvivesTwoSwitchesInARow`, 2 more |
| M19 | onErrorResume does not reset its delivered counter | **survived** | equivalent mutant, see below |
| M20 | onErrorResume swallows an invalid-request error | killed | `ResumeTimeoutTest.anInvalidRequestIsNotSwallowedByTheFallback`, 2 more |
| M21 | timeout ignores the generation of a stale timer | killed (after new test) | `ResumeTimeoutTest.aStaleTimerThatCouldNotBeCancelledInTimeIsIgnored` |
| M22 | timeout not armed when demand arrives | killed | 7 tests |
| M23 | publishOn never tops up upstream | killed | 31 tests |
| M24 | publishOn queue not bounded by capacity | killed | `PublishOnTest.anUpstreamThatOverflowsTheQueueFailsTheStreamInsteadOfGrowingIt` (only this one) |
| M25 | merge subscribes one source at a time | killed (after new test) | `FlatMapTest.mergeSubscribesToEverySourceImmediatelyUnlikeConcat` |

Notes on the results:

- **M19 is an equivalent mutant.** `produced = 0` after switching to the fallback is never read again: one
  `onErrorResume` switches at most once (a second error passes through), so the stale counter cannot change behaviour. I
  first thought it would be visible with two chained `onErrorResume`s, but each operator has its own counter. I kept the
  line as defensive hygiene, not because a test needs it.
- **M17, M21 and M25 were real gaps.** M25: `merge` silently degrading to `concat` still passed every test, because the
  items and per-source order are the same; nothing checked that all sources are subscribed at once. M21: with
  `TestScheduler`, cancelling a timer is synchronous, so the "timer already queued when the item wins the race" path was
  never exercised; the new test uses a scheduler whose `cancel()` does nothing and fires the stale timer by hand. M17: a
  publisher that calls `onSubscribe` after the downstream already cancelled; the new test captures the inner subscriber
  and delivers a late subscription.
- Several kills come from one test only (M03, M06, M09, M10, M24): those behaviours are pinned by a single targeted test.
- Mutations M17 and M21 guard races. The new tests exercise them deterministically by constructing the interleaving by
  hand, which shows the guard is needed, not that the real race is likely.

## Known limits / not verified

- **The official Reactive Streams TCK was not run.** `Source` wraps foreign publishers (`Source.from`) without policing
  them. The rule harness here is mine and checks only the *subscriber-visible* behaviour listed above. I have not mapped
  every numbered rule of the spec to a test, so no claim of full compliance is made. Not covered at all: rules for
  Processors (nothing here is one), behaviour of a foreign `Subscriber` that breaks the rules (e.g. one that throws from
  `onNext`; `Serializer` reports such exceptions to the thread's uncaught-exception handler, other operators do not
  guard), and subscribing the same subscriber instance twice.
- Concurrency tests assert invariants on a handful of thread interleavings per run; they cannot prove the absence of races.
  No `jcstress`, no long soak runs. The tests ran green repeatedly on one Windows machine; flakiness over many CI runs is
  not established.
- `buffer(n)`: if `k * n` overflows, upstream is asked for `Long.MAX_VALUE`, so in theory more than `k` buffers could be
  produced against demand `k` after about 2^63 items. Unreachable in practice, not handled.
- Errors are fail-fast in `flatMap`, items already queued are dropped (no `delayError` option). `publishOn` errors are
  delayed behind queued items. A late error after cancel is dropped silently (no global error hook).
- `onErrorResume` takes a function from the error to a fallback publisher; no type filter, no retry.
- `timeout` is per-item (idle time while demand is outstanding), with no overall deadline and no fallback publisher.
- No hot sources, subjects, `observeOn`/`subscribeOn` split, fusion, context, tracing, `zip`, `window`, `retry`, `groupBy`.
- Performance was not measured at all. Several operators allocate per signal (`timeout` allocates a task per signal).
- Only JDK 17 on Windows was run locally. The CI workflow is included, and it ran green on the first run on GitHub Actions (Java 17 and 21, ubuntu-latest, 2026-10-20).

## Layout

```
src/main/java/com/github/sup3rmar10/flowstreams/   Source, Demand, IterableSource, Link, Map/Filter/Take/Buffer/FlatMap/
                                                   OnErrorResume/Timeout/PublishOn ops, Serializer, Scheduler, TestScheduler, Demo
src/test/java/...                                  SpecComplianceTest + TestSubscriber (the harness), operator, concurrency, integration tests
scripts/mutation_check.py                          the mutation check described above
```

MIT licensed.
