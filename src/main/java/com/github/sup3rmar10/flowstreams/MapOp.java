package com.github.sup3rmar10.flowstreams;

import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.function.Function;

final class MapOp<T, R> extends Source<R> {
    private final Source<T> source;
    private final Function<? super T, ? extends R> mapper;

    MapOp(Source<T> source, Function<? super T, ? extends R> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected void subscribeActual(Flow.Subscriber<? super R> down) {
        source.subscribe(new Sub<T, R>(down, mapper));
    }

    private static final class Sub<T, R> extends Link<T, R> {
        private final Function<? super T, ? extends R> mapper;

        Sub(Flow.Subscriber<? super R> down, Function<? super T, ? extends R> mapper) {
            super(down);
            this.mapper = mapper;
        }

        @Override
        protected void next(T t) {
            R r;
            try {
                r = Objects.requireNonNull(mapper.apply(t), "the mapper returned null");
            } catch (Throwable e) {
                fail(e);
                return;
            }
            down.onNext(r);
        }
    }
}
