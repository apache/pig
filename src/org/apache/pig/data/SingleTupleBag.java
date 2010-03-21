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

/**
 * 
 */
package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;

/**
 * A simple performant implementation of the DataBag
 * interface which only holds a single tuple. This will
 * be used from POPreCombinerLocalRearrange and wherever else
 * a single Tuple non-serializable DataBag is required.
 */
public class SingleTupleBag implements DataBag {

    private static final long serialVersionUID = 1L;
    Tuple item;

    public SingleTupleBag(Tuple t) {
        item = t;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#add(org.apache.pig.data.Tuple)
     * NOTE: It is the user's responsibility to ensure only a single
     * Tuple is ever added into a SingleTupleBag
     */
    @Override
    public void add(Tuple t) {
        item = t;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#addAll(org.apache.pig.data.DataBag)
     */
    @Override
    public void addAll(DataBag b) {
        throw new RuntimeException("Cannot create SingleTupleBag from another DataBag");
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#clear()
     */
    @Override
    public void clear() {
        throw new RuntimeException("Cannot clear SingleTupleBag");
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#isDistinct()
     */
    @Override
    public boolean isDistinct() {
        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#isSorted()
     */
    @Override
    public boolean isSorted() {
        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#iterator()
     */
    @Override
    public Iterator<Tuple> iterator() {
        return new TBIterator();
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#markStale(boolean)
     */
    @Override
    public void markStale(boolean stale) {
        throw new RuntimeException("SingleTupleBag cannot be marked stale");
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#size()
     */
    @Override
    public long size() {
        return 1;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.impl.util.Spillable#getMemorySize()
     */
    @Override
    public long getMemorySize() {
        return 0;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.impl.util.Spillable#spill()
     */
    @Override
    public long spill() {
        return 0;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
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
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(size());
        Iterator<Tuple> it = iterator();
        while (it.hasNext()) {
            Tuple item = it.next();
            item.write(out);
        }    
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Object o) {
        // TODO Auto-generated method stub
        return 0;
    }

    public boolean equals(Object o){
        // TODO: match to compareTo if it is updated
        return true;
    }

    public int hashCode() {
        return 42; 
    }

    class TBIterator implements Iterator<Tuple> {
        boolean nextDone = false;
        /* (non-Javadoc)
         * @see java.util.Iterator#hasNext()
         */
        @Override
        public boolean hasNext() {
            return !nextDone;
        }

        /* (non-Javadoc)
         * @see java.util.Iterator#next()
         */
        @Override
        public Tuple next() {
            nextDone = true;
            return item;

        }

        /* (non-Javadoc)
         * @see java.util.Iterator#remove()
         */
        @Override
        public void remove() {
            throw new RuntimeException("SingleTupleBag.iterator().remove() is not allowed");    
        }
    }

    /**
     * Write the bag into a string. */
    @Override
    public String toString() {
        return "{" + item + "}";
    }
}
