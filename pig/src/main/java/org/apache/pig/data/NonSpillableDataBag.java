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
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;


/**
 * An unordered collection of Tuples (possibly) with multiples.  The tuples
 * are stored in an ArrayList, since there is no concern for order or
 * distinctness. The implicit assumption is that the user of this class
 * is storing only those many tuples as will fit in memory - no spilling
 * will be done on this bag to disk.
 */
public class NonSpillableDataBag implements DataBag {
    // the reason this class does NOT extend DefaultAbstractBag
    // is that we don't want to bloat this class with members it
    // does not need (DefaultAbstractBag has many members related
    // to spilling which are not needed here)
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    private List<Tuple> mContents;    

    public NonSpillableDataBag() {
        mContents = new ArrayList<Tuple>();
    }

    /**
     * Use this constructor if you know upfront how many tuples you are going
     * to put in this bag.
     * @param tupleCount
     */
    public NonSpillableDataBag(int tupleCount){
        mContents = new ArrayList<Tuple>(tupleCount);
    }
    
    /**
     * This constructor creates a bag out of an existing list
     * of tuples by taking ownership of the list and NOT
     * copying the contents of the list.
     * @param listOfTuples List<Tuple> containing the tuples
     */
    public NonSpillableDataBag(List<Tuple> listOfTuples) {
        mContents = listOfTuples;
    }

    public boolean isSorted() {
        return false;
    }
    
    public boolean isDistinct() {
        return false;
    }
    
    public Iterator<Tuple> iterator() {
        return new NonSpillableDataBagIterator();
    }

    /**
     * An iterator that handles getting the next tuple from the bag.
     */
    private class NonSpillableDataBagIterator implements Iterator<Tuple> {

        private int mCntr = 0;

        public boolean hasNext() { 
            return (mCntr < mContents.size());
        }

        public Tuple next() {
            // This will report progress every 1024 times through next.
            // This should be much faster than using mod.
            if ((mCntr & 0x3ff) == 0) reportProgress();

            return mContents.get(mCntr++);
        }

        /**
         * Not implemented.
         */
        public void remove() { throw new RuntimeException("Cannot remove() from NonSpillableDataBag.iterator()");}
    }    

    /**
     * Report progress to HDFS.
     */
    protected void reportProgress() {
        if (PhysicalOperator.getReporter() != null) {
            PhysicalOperator.getReporter().progress();
        }
    }

    @Override
    public void add(Tuple t) {
        mContents.add(t);
    }

    @Override
    public void addAll(DataBag b) {
        for (Tuple t : b) {
            mContents.add(t);
        }
    }

    @Override
    public void clear() {
        mContents.clear();        
    }

    @Override
    public void markStale(boolean stale) {
        throw new RuntimeException("NonSpillableDataBag cannot be marked stale");
    }

    @Override
    public long size() {
        return mContents.size();
    }

    @Override
    public long getMemorySize() {
        return 0;
    }

    @Override
    public long spill() {
        // TODO Auto-generated method stub
        return 0;
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
    
    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        return compareTo(obj) == 0;
    }

    public int hashCode() {
        return mContents.hashCode();
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

            // Ugh, this is bogus.  But I have to know if two bags have the
            // same tuples, regardless of order.  Hopefully most of the
            // time the size check above will prevent this.
            // If either bag isn't already sorted, create a sorted bag out
            // of it so I can guarantee order.           
            BagFactory factory = BagFactory.getInstance();
            
            DataBag thisClone;
            DataBag otherClone;
            thisClone = factory.newSortedBag(null);
            Iterator<Tuple> i = iterator();
            while (i.hasNext()) thisClone.add(i.next());
            if (((DataBag) other).isSorted() || ((DataBag) other).isDistinct()) {
                otherClone = bOther;
            } else {
                otherClone = factory.newSortedBag(null);
                i = bOther.iterator();
                while (i.hasNext()) otherClone.add(i.next());
            }
            Iterator<Tuple> thisIt = thisClone.iterator();
            Iterator<Tuple> otherIt = otherClone.iterator();
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
     * Write the bag into a string. */
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
    
}

