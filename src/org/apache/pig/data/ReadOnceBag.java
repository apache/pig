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
import java.util.Iterator;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackageLite;
import org.apache.pig.impl.io.NullableTuple;

/**
 * This bag is specifically created for use by POPackageLite. So it has three 
 * properties, the NullableTuple iterator, the key (Object) and the keyInfo 
 * (Map<Integer, Pair<Boolean, Map<Integer, Integer>>>) all three 
 * of which are required in the constructor call. This bag does not store 
 * the tuples in memory, but has access to an iterator typically provided by 
 * Hadoop. Use this when you already have an iterator over tuples and do not 
 * want to copy over again to a new bag.
 */
public class ReadOnceBag implements DataBag {

    // The Package operator that created this
    POPackageLite pkg;
    
    //The iterator of Tuples. Marked transient because we will never serialize this.
    transient Iterator<NullableTuple> tupIter;
    
    // The key being worked on
    Object key;

    /**
     * 
     */
    private static final long serialVersionUID = 2L;

    /**
     * This constructor creates a bag out of an existing iterator
     * of tuples by taking ownership of the iterator and NOT
     * copying the elements of the iterator.
     * @param pkg POPackageLite
     * @param tupIter Iterator<NullableTuple>
     * @param key Object
     */
    public ReadOnceBag(POPackageLite pkg, Iterator<NullableTuple> tupIter, Object key) {
        this.pkg = pkg;
        this.tupIter = tupIter;
        this.key = key;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.impl.util.Spillable#getMemorySize()
     */
    @Override
    public long getMemorySize() {
        throw new RuntimeException("ReadOnceBag does not support getMemorySize operation");
    }

    /* (non-Javadoc)
     * @see org.apache.pig.impl.util.Spillable#spill()
  
     */
    @Override
    public long spill() {
        throw new RuntimeException("ReadOnceBag does not support spill operation");
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#add(org.apache.pig.data.Tuple)
     */
    @Override
    public void add(Tuple t) {
        throw new RuntimeException("ReadOnceBag does not support add operation");		
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#addAll(org.apache.pig.data.DataBag)
     */
    @Override
    public void addAll(DataBag b) {
        throw new RuntimeException("ReadOnceBag does not support addAll operation");
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#clear()
     */
    @Override
    public void clear() {
        throw new RuntimeException("ReadOnceBag does not support clear operation");
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#isDistinct()
     */
    @Override
    public boolean isDistinct() {
        throw new RuntimeException("ReadOnceBag does not support isDistinct operation");
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#isSorted()
     */
    @Override
    public boolean isSorted() {
        throw new RuntimeException("ReadOnceBag does not support isSorted operation");
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#iterator()
     */
    @Override
    public Iterator<Tuple> iterator() {
        return new ReadOnceBagIterator();
    }

    /* (non-Javadoc)
	 * @see org.apache.pig.data.DataBag#markStale(boolean)
     */
    @Override
    public void markStale(boolean stale) {
        throw new RuntimeException("ReadOnceBag does not support markStale operation");
    }

    /* (non-Javadoc)
     * @see org.apache.pig.data.DataBag#size()
     */
    @Override
    public long size() {
        throw new RuntimeException("ReadOnceBag does not support size operation");
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        throw new RuntimeException("ReadOnceBag does not support readFields operation");
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        int errCode = 2142;
        String msg = "ReadOnceBag should never be serialized.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     * This has to be defined since DataBag implements 
     * Comparable although, in this case we cannot really compare.
     */
    @Override
    public int compareTo(Object o) {
        throw new RuntimeException("ReadOnceBags cannot be compared");
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof ReadOnceBag)
        {
            if(pkg.getKeyTuple())
            {
                if(tupIter == ((ReadOnceBag)other).tupIter && pkg.getKeyTuple() == ((ReadOnceBag)other).pkg.getKeyTuple() && pkg.getKeyAsTuple().equals(((ReadOnceBag)other).pkg.getKeyAsTuple()))
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                if(tupIter == ((ReadOnceBag)other).tupIter && pkg.getKey().equals(((ReadOnceBag)other).pkg.getKey()))
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
    	int hash = 7;
        if(pkg.getKeyTuple())
        {
            hash = hash*31 + pkg.getKeyAsTuple().hashCode();
        }
        else
        {
        	hash = hash*31 + pkg.getKey().hashCode();
        }
        return hash;
    }

    class ReadOnceBagIterator implements Iterator<Tuple>
    {
        /* (non-Javadoc)
         * @see java.util.Iterator#hasNext()
         */
        @Override
        public boolean hasNext() {
            return tupIter.hasNext();
        }

        /* (non-Javadoc)
         * @see java.util.Iterator#next()
         */
        @Override
        public Tuple next() {
            NullableTuple ntup = tupIter.next();
            int index = ntup.getIndex();
            Tuple ret = null;
            try {
                ret = pkg.getValueTuple(ntup, index, key);
            } catch (ExecException e)
            {
            	throw new RuntimeException("ReadOnceBag failed to get value tuple : "+e.toString());
            }
            return ret;
        }
		
        /* (non-Javadoc)
         * @see java.util.Iterator#remove()
         */
        @Override
        public void remove() {
            throw new RuntimeException("ReadOnceBag.iterator().remove() is not allowed");    
        }
	}
}

