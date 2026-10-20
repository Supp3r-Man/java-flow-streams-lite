package com.github.sup3rmar10.flowstreams;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Three small scenarios, all with synthetic data:
 * 1. a fast producer (a million-element range) against a deliberately slow consumer, across an async boundary:
 *    shows how far ahead the producer is ever allowed to run;
 * 2. merge versus concat, and buffer;
 * 3. a 5 second timeout that fires in virtual time (no waiting).
 */
public final class Demo {

    public static void main(String[] args) throws Exception {
        slowConsumer();
        mergeConcatBuffer();
        virtualTimeout();
    }

    private static void slowConsumer() throws Exception {
        System.out.println("== 1. fast producer, slow consumer, publishOn capacity 16");
        final int capacity = 16;
        final int wanted = 300;
        AtomicLong produced = new AtomicLong();
        AtomicLong consumed = new AtomicLong();
        AtomicLong maxAhead = new AtomicLong();
        ExecutorService pool = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "demo-consumer-side");
            t.setDaemon(true);
            return t;
        });
        CountDownLatch finished = new CountDownLatch(1);
        try {
            Source.range(0, 1_000_000)
                    .map(i -> {
                        long p = produced.incrementAndGet();
                        maxAhead.accumulateAndGet(p - consumed.get(), Math::max);
                        return i;
                    })
                    .publishOn(pool, capacity)
                    .take(wanted)
                    .subscribe(new Flow.Subscriber<Long>() {
                        private Flow.Subscription s;

                        @Override
                        public void onSubscribe(Flow.Subscription s) {
                            this.s = s;
                            s.request(1);
                        }

                        @Override
                        public void onNext(Long item) {
                            consumed.incrementAndGet();
                            try {
                                Thread.sleep(1); // the slow consumer
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            s.request(1);
                        }

                        @Override
                        public void onError(Throwable t) {
                            System.out.println("error: " + t);
                            finished.countDown();
                        }

                        @Override
                        public void onComplete() {
                            finished.countDown();
                        }
                    });
            if (!finished.await(60, TimeUnit.SECONDS)) {
                throw new IllegalStateException("demo did not finish");
            }
        } finally {
            pool.shutdownNow();
        }
        System.out.println("consumer handled " + consumed.get() + " items, one request(1) at a time");
        System.out.println("producer generated " + produced.get() + " of 1000000 possible elements");
        System.out.println("max (produced - consumed) ever observed: " + maxAhead.get() + "  (hard bound = capacity = " + capacity + ")");
        if (maxAhead.get() > capacity) {
            throw new IllegalStateException("backpressure bound violated");
        }
        System.out.println("bounded memory: yes");
    }

    private static void mergeConcatBuffer() {
        System.out.println();
        System.out.println("== 2. concat, flatMap(concurrency 2), buffer(4)");
        Source<Integer> a = Source.just(1, 2, 3);
        Source<Integer> b = Source.just(10, 20, 30);
        System.out.println("concat(a, b)           = " + collect(Source.concat(a, b)));
        System.out.println("range(1,6).flatMap(x -> just(x, -x), 2) = "
                + collect(Source.range(1, 6).flatMap(x -> Source.just(x, -x), 2)));
        System.out.println("range(1,10).buffer(4)  = " + collect(Source.range(1, 10).buffer(4)));
    }

    private static <T> List<T> collect(Source<T> source) {
        java.util.ArrayList<T> out = new java.util.ArrayList<>();
        source.subscribe(new Flow.Subscriber<T>() {
            @Override
            public void onSubscribe(Flow.Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(T item) {
                out.add(item);
            }

            @Override
            public void onError(Throwable t) {
                throw new IllegalStateException(t);
            }

            @Override
            public void onComplete() {
            }
        });
        return out;
    }

    private static void virtualTimeout() {
        System.out.println();
        System.out.println("== 3. timeout(5 s) driven by a virtual clock (no sleeping)");
        TestScheduler clock = new TestScheduler();
        AtomicReference<String> outcome = new AtomicReference<>("waiting");
        Source.<Integer>never().timeout(Duration.ofSeconds(5), clock).subscribe(new Flow.Subscriber<Integer>() {
            @Override
            public void onSubscribe(Flow.Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable t) {
                outcome.set("error: " + t);
            }

            @Override
            public void onComplete() {
                outcome.set("completed");
            }
        });
        clock.advanceBy(Duration.ofMillis(4999));
        System.out.println("after 4999 ms: " + outcome.get());
        clock.advanceBy(Duration.ofMillis(1));
        System.out.println("after 5000 ms: " + outcome.get());
    }

    private Demo() {
    }
}
