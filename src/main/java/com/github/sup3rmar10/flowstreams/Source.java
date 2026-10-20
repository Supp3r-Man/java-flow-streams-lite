package com.github.sup3rmar10.flowstreams;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A cold, Flux-like publisher of 0..n items followed by completion or an error, built on
 * {@link java.util.concurrent.Flow}. Every subscribe() starts a fresh run. Operators are lazy: nothing happens
 * until a Subscriber subscribes and calls request(n).
 *
 * <p>Not a replacement for Reactor or RxJava: no hot sources, no schedulers beyond publishOn, no fusion, no
 * context, no tracing. See the README for the list of what is and is not checked.
 */
public abstract class Source<T> implements Flow.Publisher<T> {

    /** Implemented by sources and operators. The subscriber is never null here. */
    protected abstract void subscribeActual(Flow.Subscriber<? super T> subscriber);

    /** Rule 1.9 (and 2.13 for the argument): a null subscriber is a NullPointerException, not a signal. */
    @Override
    public final void subscribe(Flow.Subscriber<? super T> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber");
        subscribeActual(subscriber);
    }

    // ---------------------------------------------------------------- factories

    @SafeVarargs
    public static <T> Source<T> just(T... items) {
        return new IterableSource<>(Arrays.asList(items.clone()));
    }

    public static <T> Source<T> fromIterable(Iterable<? extends T> iterable) {
        return new IterableSource<>(Objects.requireNonNull(iterable, "iterable"));
    }

    /** Longs start, start+1, ... start+count-1. */
    public static Source<Long> range(long start, long count) {
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        if (count > 0 && start > Long.MAX_VALUE - (count - 1)) {
            throw new IllegalArgumentException("range overflows long");
        }
        return new IterableSource<>(() -> new Iterator<Long>() {
            private long next = start;
            private long remaining = count;

            @Override
            public boolean hasNext() {
                return remaining > 0;
            }

            @Override
            public Long next() {
                if (remaining <= 0) {
                    throw new NoSuchElementException();
                }
                remaining--;
                return next++;
            }
        });
    }

    public static <T> Source<T> empty() {
        return new Source<>() {
            @Override
            protected void subscribeActual(Flow.Subscriber<? super T> s) {
                Subscriptions.complete(s);
            }
        };
    }

    public static <T> Source<T> error(Throwable error) {
        Objects.requireNonNull(error, "error");
        return new Source<>() {
            @Override
            protected void subscribeActual(Flow.Subscriber<? super T> s) {
                Subscriptions.error(s, error);
            }
        };
    }

    /** Signals onSubscribe and then nothing, ever. */
    public static <T> Source<T> never() {
        return new Source<>() {
            @Override
            protected void subscribeActual(Flow.Subscriber<? super T> s) {
                s.onSubscribe(Subscriptions.NOOP);
            }
        };
    }

    /** Adapts any Flow.Publisher. It is trusted to follow the spec: this library does not police foreign publishers. */
    @SuppressWarnings("unchecked")
    public static <T> Source<T> from(Flow.Publisher<? extends T> publisher) {
        Objects.requireNonNull(publisher, "publisher");
        if (publisher instanceof Source) {
            return (Source<T>) publisher;
        }
        return new Source<>() {
            @Override
            protected void subscribeActual(Flow.Subscriber<? super T> s) {
                publisher.subscribe(s);
            }
        };
    }

    /** Subscribes to all sources at once (maxConcurrency = number of sources) and interleaves their items. */
    @SafeVarargs
    public static <T> Source<T> merge(Source<? extends T>... sources) {
        List<Source<? extends T>> list = Arrays.asList(sources.clone());
        if (list.isEmpty()) {
            return empty();
        }
        return Source.<Source<? extends T>>fromIterable(list).flatMap(s -> s, list.size());
    }

    /** Subscribes to the sources one after another, in order; the next one is not subscribed until the previous completes. */
    @SafeVarargs
    public static <T> Source<T> concat(Source<? extends T>... sources) {
        List<Source<? extends T>> list = Arrays.asList(sources.clone());
        if (list.isEmpty()) {
            return empty();
        }
        return Source.<Source<? extends T>>fromIterable(list).flatMap(s -> s, 1);
    }

    // ---------------------------------------------------------------- operators

    public final <R> Source<R> map(Function<? super T, ? extends R> mapper) {
        return new MapOp<>(this, Objects.requireNonNull(mapper, "mapper"));
    }

    public final Source<T> filter(Predicate<? super T> predicate) {
        return new FilterOp<>(this, Objects.requireNonNull(predicate, "predicate"));
    }

    /** Emits at most n items, then cancels upstream and completes. Never asks upstream for more than n in total. */
    public final Source<T> take(long n) {
        if (n < 0) {
            throw new IllegalArgumentException("n must be >= 0");
        }
        return new TakeOp<>(this, n);
    }

    /** flatMap with at most {@code maxConcurrency} inner publishers subscribed at once and the default inner prefetch (16). */
    public final <R> Source<R> flatMap(Function<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency) {
        return flatMap(mapper, maxConcurrency, 16);
    }

    /**
     * @param maxConcurrency how many inner publishers may be subscribed at the same time (also how many outer items
     *                       are requested up front); 1 gives concat semantics.
     * @param prefetch       how many items each inner publisher may have outstanding / queued at once.
     */
    public final <R> Source<R> flatMap(Function<? super T, ? extends Flow.Publisher<? extends R>> mapper,
                                       int maxConcurrency, int prefetch) {
        Objects.requireNonNull(mapper, "mapper");
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency must be >= 1");
        }
        if (prefetch < 1) {
            throw new IllegalArgumentException("prefetch must be >= 1");
        }
        return new FlatMapOp<>(this, mapper, maxConcurrency, prefetch);
    }

    /** Groups items into lists of exactly n; a final shorter list is emitted on completion. */
    public final Source<List<T>> buffer(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("n must be >= 1");
        }
        return new BufferOp<>(this, n);
    }

    public final Source<T> onErrorResume(Function<? super Throwable, ? extends Flow.Publisher<? extends T>> fallback) {
        return new OnErrorResumeOp<>(this, Objects.requireNonNull(fallback, "fallback"));
    }

    /**
     * Fails with a TimeoutException if no item arrives within {@code timeout} while downstream demand is outstanding.
     * The clock is the injected scheduler, so tests can drive it by hand.
     */
    public final Source<T> timeout(Duration timeout, Scheduler scheduler) {
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(scheduler, "scheduler");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        return new TimeoutOp<>(this, timeout.toNanos(), scheduler);
    }

    /**
     * Async boundary: downstream signals are delivered from {@code executor}; at most {@code capacity} items are
     * ever queued (the queue is bounded and upstream is only asked for what fits).
     */
    public final Source<T> publishOn(Executor executor, int capacity) {
        Objects.requireNonNull(executor, "executor");
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1");
        }
        return new PublishOnOp<>(this, executor, capacity);
    }

    @SuppressWarnings("unchecked")
    public final Source<T> mergeWith(Source<? extends T> other) {
        return merge(this, other);
    }

    @SuppressWarnings("unchecked")
    public final Source<T> concatWith(Source<? extends T> other) {
        return concat(this, other);
    }
}
