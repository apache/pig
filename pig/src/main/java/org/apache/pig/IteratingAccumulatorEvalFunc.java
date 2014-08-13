/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * This class provides a much more intuitive way to write Accumulator UDFs.<br>
 * For example, you could express IsEmpty as follows:
 * <pre><code>public class IsEmpty extends IteratingAccumulatorEvalFunc<Boolean> {
 *     public Boolean exec(Iterator<Tuple> iter) throws IOException {
 *         return !iter.hashNext();
 *     }
 * }</code></pre>
 * Count could be implemented as follows:
 * <pre><code>public class Count extends IteratingAccumulatorEvalFunc<Long> {
 *     public Long exec(Iterator<Tuple> iter) throws IOException {
 *         long ct = 0;
 *         for (; iter.hasNext(); iter.next()) {
 *             ct++;
 *         }
 *         return ct;
 *     }
 * }</code></pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class IteratingAccumulatorEvalFunc<T> extends AccumulatorEvalFunc<T> implements TerminatingAccumulator<T> {
    private boolean isInitialized = false;
    private DelayedQueueIterator dqi;
    private BlockingQueue<Tuple> queue;
    private volatile boolean isFinished = false;
    private volatile boolean noMoreValues = false;
    private volatile boolean exceptionThrown = false;
    private T returnValue;
    private Thread executionThread;
    private Exception executionThreadException;

    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    private static final long WAIT_TO_OFFER = 500L;
    private static final long WAIT_TO_POLL = 500L;
    private static final long WAIT_TO_JOIN = 500L;

    private void initialize() {
        dqi = new DelayedQueueIterator();
        queue = new LinkedBlockingQueue<Tuple>(10000);

        executionThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    returnValue = exec(dqi);
                    isFinished = true;
                } catch (Exception e) {
                    executionThreadException = e;
                    exceptionThrown = true;
                }
            }
        });
        executionThread.start();

        isInitialized = true;
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public void accumulate(Tuple input) throws IOException {
        if (!isInitialized)
            initialize();

        for (Tuple t : (DataBag)input.get(0)) {
            if (isFinished)
                return;

            boolean added = false;
            while (!isFinished && !added && !exceptionThrown)
                try {
                    added = queue.offer(t, WAIT_TO_OFFER, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                } //TODO handle the exception?

            if (exceptionThrown)
                throw new RuntimeException("Exception thrown in thread: ", executionThreadException);
        }
    }

    @Override
    public T getValue() {
        noMoreValues = true;

        do {
            if (exceptionThrown)
                throw new RuntimeException("Exception thrown in thread: ", executionThreadException);

            try {
                executionThread.join(WAIT_TO_JOIN);
            } catch (InterruptedException e) {
            } //TODO handle the exception?
        } while (executionThread.isAlive());

        return returnValue;
    }

    @Override
    public void cleanup() {
        returnValue = null;
        dqi = null;
        queue = null;
        isFinished = false;
        noMoreValues = false;
        executionThread = null;
        isInitialized = false;
    }

    public abstract T exec(Iterator<Tuple> iter) throws IOException;

    private class DelayedQueueIterator implements Iterator<Tuple> {
        private Tuple next;

        @Override
        public boolean hasNext() {
            if (next != null)
                return true;

            while (!noMoreValues) {
                try {
                    next = queue.poll(WAIT_TO_POLL, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (next != null)
                    return true;
            }

            next = queue.poll();

            if (next != null)
                return true;

            return false;
        }

       @Override
       public Tuple next() {
           Tuple t = next;

           if (t == null)
               throw new RuntimeException("Entered inconsistent state!");

           next = null;

           return t;
       }

       @Override
       public void remove() {}
    }
}
