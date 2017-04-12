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
package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;


/**
 * An ordered collection of Tuples (possibly) with multiples.  Data is
 * stored in a priority queue as it comes in, and only sorted when iterator is requested.
 *
 * LimitedSortedDataBag is not spillable.
 *
 * We allow a user defined comparator, but provide a default comparator in
 * cases where the user doesn't specify one.
 */
public class LimitedSortedDataBag implements DataBag {

    private static final Log log = LogFactory.getLog(LimitedSortedDataBag.class);
    private static final long serialVersionUID = 1L;

    private final Comparator<Tuple> mComp;
	private final PriorityQueue<Tuple> priorityQ;
	private final long limit;

	/**
	 * @param comp Comparator to use to do the sorting.
	 * If null, DefaultComparator will be used.
	 */
	public LimitedSortedDataBag(Comparator<Tuple> comp, long limit) {
	    this.mComp = comp == null ? new DefaultComparator() : comp;
	    this.limit = limit;
	    this.priorityQ = new PriorityQueue<Tuple>(
	            (int)limit, getReversedComparator(mComp));
	}

	/**
     * Get the number of elements in the bag in memory.
     * @return number of elements in the bag
     */
	@Override
	public long size() {
	   return priorityQ.size();
	}

	/**
     * Find out if the bag is sorted.
     * @return true if this is a sorted data bag, false otherwise.
     */
	@Override
	public boolean isSorted() {
	    return true;
	}

	/**
     * Find out if the bag is distinct.
     * @return true if the bag is a distinct bag, false otherwise.
     */
	@Override
	public boolean isDistinct() {
	    return false;
	}

	/**
     * Get an iterator to the bag. For default and distinct bags,
     * no particular order is guaranteed. For sorted bags the order
     * is guaranteed to be sorted according
     * to the provided comparator.
     * @return tuple iterator
     */
	@Override
	public Iterator<Tuple> iterator() {
	    return new LimitedSortedDataBagIterator();
	}

	/**
     * Add a tuple to the bag.
     * @param t tuple to add.
     */
    @Override
    public void add(Tuple t) {
        priorityQ.add(t);
        if (priorityQ.size() > limit) {
            priorityQ.poll();
        }
    }

    /**
     * Add contents of a bag to the bag.
     * @param b bag to add contents of.
     */
    @Override
    public void addAll(DataBag b) {
        Iterator<Tuple> it = b.iterator();
        while(it.hasNext()) {
            add(it.next());
        }
    }

    /**
     * Clear out the contents of the bag, both on disk and in memory.
     * Any attempts to read after this is called will produce undefined
     * results.
     */
    @Override
    public void clear() {
        priorityQ.clear();
    }

    /**
     * Write a bag's contents to disk.
     * @param out DataOutput to write data to.
     * @throws IOException (passes it on from underlying calls).
     */
    public void write(DataOutput out) throws IOException {
        // We don't care whether this bag was sorted or distinct because
        // using the iterator to write it will guarantee those things come
        // correctly.  And on the other end there'll be no reason to waste
        // time re-sorting or re-applying distinct.
        out.writeLong(size());
        Iterator<Tuple> it = iterator();
        while (it.hasNext()) {
            Tuple item = it.next();
            item.write(out);
        }
    }

    /**
     * Read a bag from disk.
     * @param in DataInput to read data from.
     * @throws IOException (passes it on from underlying calls).
     */
    public void readFields(DataInput in) throws IOException {
        long size = in.readLong();

        for (long i = 0; i < size; i++) {
            try {
                Object o = DataReaderWriter.readDatum(in);
                add((Tuple)o);
            } catch (ExecException ee) {
                throw ee;
            }
        }
    }

    /**
     * Write the bag into a string.
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append('{');
        Iterator<Tuple> it = iterator();
        while ( it.hasNext() ) {
            Tuple t = it.next();
            String s = t.toString();
            sb.append(s);
            if (it.hasNext()) sb.append(",");
        }
        sb.append('}');
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Object other) {
        if (this == other)
            return 0;
        if (other instanceof DataBag) {
            DataBag bOther = (DataBag) other;
            if (this.size() != bOther.size()) {
                if (this.size() > bOther.size()) return 1;
                else return -1;
            }

            // if we got this far, both bags should have same size
            // make a LimitedSortedBag for the other bag with same comparator and limit
            // so that both bag are sorted and we can loop through both iterators
            DataBag otherCloneDataBag = new LimitedSortedDataBag(mComp, limit);
            otherCloneDataBag.addAll((DataBag) other);

            Iterator<Tuple> thisIt = this.iterator();
            Iterator<Tuple> otherIt = otherCloneDataBag.iterator();

            while (thisIt.hasNext() && otherIt.hasNext()) {
                Tuple thisT = thisIt.next();
                Tuple otherT = otherIt.next();

                int c = thisT.compareTo(otherT);
                if (c != 0) return c;
            }
            return 0;   // if we got this far, they must be equal
        } else {
            return DataType.compare(this, other);
        }
    }

    /**
     * Not implemented.
     * This is used by FuncEvalSpec.FakeDataBag.
     * @param stale Set stale state.
     */
    @Override
    public void markStale(boolean stale) {
        throw new RuntimeException("LimitedSortedDataBag cannot be marked stale");
    }

    /**
     * Not implemented.
     */
    @Override
    public long spill() {
        return 0;
    }

    /**
     * Not implemented.
     */
    @Override
    public long getMemorySize() {
        return 0;
    }

    private static class DefaultComparator implements Comparator<Tuple> {
        @Override
        @SuppressWarnings("unchecked")
        public int compare(Tuple t1, Tuple t2) {
            return t1.compareTo(t2);
        }

        @Override
        public boolean equals(Object o) {
            return false;
        }

        @Override
        public int hashCode() {
            return 42;
        }
    }

   /**
    * Since comparator in Java 1.7 does not have reversed(),
    * we need this method to get a reversed comparator for a given comparator
    * @param comp Comparator to reverse
    * @return reversed comparator
    */
    private <T> Comparator<T> getReversedComparator(final Comparator<T> comp) {

        return new Comparator<T>() {

            @Override
            public int compare(T o1, T o2) {
                return -comp.compare(o1, o2);
            }

            @Override
            public boolean equals(Object o) {
                return comp.equals(o);
            }
        };
    }

    /**
     * An iterator that handles getting the next tuple from the bag.
     * Since priority queue iterator does not return elements in any order
     * we need to dump elements in a List, sort them, and return iterator of the List
     */
    private class LimitedSortedDataBagIterator implements Iterator<Tuple> {

        private int mCntr;
        private final List<Tuple> mContents;

        public LimitedSortedDataBagIterator() {
            mCntr = 0;
            mContents = new ArrayList<>(priorityQ);
            Collections.sort(mContents, mComp);
        }

        @Override
        public boolean hasNext() {
            return (mCntr < mContents.size());
        }

        @Override
        public Tuple next() {
            // This will report progress every 1024 times through next.
            // This should be much faster than using mod.
            if ((mCntr & 0x3ff) == 0) reportProgress();

            return mContents.get(mCntr++);
        }

        /**
         * Not implemented.
         */
        public void remove() {
            throw new RuntimeException("Cannot remove() from LimitedSortedDataBag.iterator()");
        }
    }

    /**
     * Report progress to HDFS.
     */
    protected void reportProgress() {
        if (PhysicalOperator.getReporter() != null) {
            PhysicalOperator.getReporter().progress();
        }
    }
}
