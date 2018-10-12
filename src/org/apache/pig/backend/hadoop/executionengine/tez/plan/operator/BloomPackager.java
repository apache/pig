/**
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
package org.apache.pig.backend.hadoop.executionengine.tez.plan.operator;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.util.bloom.Key;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.Packager;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.bloom.BloomFilter;

public class BloomPackager extends Packager {

    private static final long serialVersionUID = 1L;
    private static final Result RESULT_EMPTY = new Result(POStatus.STATUS_NULL, null);
    private static final Result RESULT_EOP = new Result(POStatus.STATUS_EOP, null);

    private boolean bloomCreatedInMap;
    private int vectorSizeBytes;
    private int numBloomFilters;
    private int numHash;
    private int hashType;

    private transient ByteArrayOutputStream baos;
    private transient BloomFilter[] bloomFilters;
    private transient int nextFilterIdx;

    public BloomPackager(boolean bloomCreatedInMap, int numBloomFilters, int vectorSizeBytes,
            int numHash, int hashType) {
        super();
        this.bloomCreatedInMap = bloomCreatedInMap;
        this.vectorSizeBytes = vectorSizeBytes;
        this.numHash = numHash;
        this.hashType = hashType;
        this.numBloomFilters = numBloomFilters;
    }

    @Override
    public void attachInput(Object key, DataBag[] bags, boolean[] readOnce)
            throws ExecException {
        this.key = key;
        this.bags = bags;
        this.readOnce = readOnce;
        // Bag can be read directly and need not be materialized again
    }

    @Override
    public Result getNext() throws ExecException {
        try {
            if (bags == null) {
                return new Result(POStatus.STATUS_EOP, null);
            }
            if (bloomCreatedInMap) {
                return combineBloomFilters();
            } else {
                if (parent.isEndOfAllInput()) {
                    return retrieveBloomFilter();
                }
                if (!bags[0].iterator().hasNext()) {
                    return new Result(POStatus.STATUS_EOP, null);
                }
                if (bloomFilters == null) { // init
                    bloomFilters = new BloomFilter[numBloomFilters];
                }
                // Create the bloom filters from the keys
                Tuple tup = bags[0].iterator().next();
                addKeyToBloomFilter(key, (int) tup.get(0));
                detachInput();
                return RESULT_EMPTY;
            }
        } catch (IOException e) {
            throw new ExecException("Error while constructing final bloom filter", e);
        }
    }

    private Result combineBloomFilters() throws IOException {
        // We get a bag of bloom filters. combine them into one
        Iterator<Tuple> iter = bags[0].iterator();
        Tuple tup = iter.next();
        DataByteArray bloomBytes = (DataByteArray) tup.get(0);
        BloomFilter bloomFilter = BloomFilter.bloomIn(bloomBytes);
        while (iter.hasNext()) {
            tup = iter.next();
            bloomFilter.or(BloomFilter.bloomIn((DataByteArray) tup.get(0)));
        }

        Object partition = key;
        detachInput(); // Free up the key and bags reference

        return getSerializedBloomFilter(partition, bloomFilter, bloomBytes.get().length);
    }

    private Result getSerializedBloomFilter(Object partition,
            BloomFilter bloomFilter, int serializedSize) throws ExecException,
            IOException {
        if (baos == null) {
            baos = new ByteArrayOutputStream(serializedSize);
        }
        baos.reset();
        DataOutputStream dos = new DataOutputStream(baos);
        bloomFilter.write(dos);
        dos.flush();

        Tuple res = mTupleFactory.newTuple(2);
        res.set(0, partition);
        res.set(1, new DataByteArray(baos.toByteArray()));

        Result r = new Result();
        r.result = res;
        r.returnStatus = POStatus.STATUS_OK;
        return r;
    }

    private void addKeyToBloomFilter(Object key, int partition) throws ExecException {
        Key k = new Key(((DataByteArray)key).get());
        BloomFilter filter = bloomFilters[partition];
        if (filter == null) {
            filter = new BloomFilter(vectorSizeBytes * 8, numHash, hashType);
            bloomFilters[partition] = filter;
        }
        filter.add(k);
    }

    private Result retrieveBloomFilter() throws IOException  {
        while (nextFilterIdx < numBloomFilters) {
            if (bloomFilters[nextFilterIdx] != null) {
                return getSerializedBloomFilter(nextFilterIdx, bloomFilters[nextFilterIdx++], vectorSizeBytes + 64);
            } else {
                nextFilterIdx++;
            }
        }
        return RESULT_EOP;
    }

    public boolean isBloomCreatedInMap() {
        return bloomCreatedInMap;
    }
}
