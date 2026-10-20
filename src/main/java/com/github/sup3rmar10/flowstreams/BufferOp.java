package com.github.sup3rmar10.flowstreams;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

/**
 * buffer(n): one downstream request(k) becomes an upstream request(k * n) (saturating), so upstream is never
 * asked for items that no downstream demand could consume. A partial buffer at completion is emitted using the
 * demand left over from that arithmetic; a partial buffer at error time is dropped.
 */
final class BufferOp<T> extends Source<List<T>> {
    private final Source<T> source;
    private final int size;

    BufferOp(Source<T> source, int size) {
        this.source = source;
        this.size = size;
    }

    @Override
    protected void subscribeActual(Flow.Subscriber<? super List<T>> down) {
        source.subscribe(new Sub<T>(down, size));
    }

    private static final class Sub<T> extends Link<T, List<T>> {
        private final int size;
        private List<T> buffer;

        Sub(Flow.Subscriber<? super List<T>> down, int size) {
            super(down);
            this.size = size;
            this.buffer = new ArrayList<>(Math.min(size, 64));
        }

        @Override
        protected void next(T t) {
            buffer.add(t);
            if (buffer.size() == size) {
                List<T> out = buffer;
                buffer = new ArrayList<>(Math.min(size, 64));
                down.onNext(out);
            }
        }

        @Override
        protected void complete() {
            done = true;
            List<T> rest = buffer;
            buffer = null;
            if (!rest.isEmpty()) {
                down.onNext(rest);
                if (cancelled) {
                    return;
                }
            }
            down.onComplete();
        }

        @Override
        public void request(long n) {
            up.request(n <= 0 ? n : Demand.multiply(n, size));
        }
    }
}
