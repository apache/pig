package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.util.Iterator;

abstract class IteratorTransform<IN, OUT> implements Iterator<OUT> {
    private Iterator<IN> delegate;

    public IteratorTransform(Iterator<IN> delegate) {
        super();
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public OUT next() {
        return transform(delegate.next());
    }

    abstract protected OUT transform(IN next);

    @Override
    public void remove() {
        delegate.remove();
    }

}