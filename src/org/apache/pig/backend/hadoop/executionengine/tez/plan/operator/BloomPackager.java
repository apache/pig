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
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.Packager;
import org.apache.pig.builtin.BuildBloomBase;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

public class BloomPackager extends Packager {

    private static final long serialVersionUID = 1L;

    private boolean bloomCreatedInMap;
    private int vectorSizeBytes;
    private int numHash;
    private int hashType;
    private byte bloomKeyType;
    private boolean isCombiner;

    private transient ByteArrayOutputStream baos;
    private transient Iterator<Object> distinctKeyIter;

    public BloomPackager(boolean bloomCreatedInMap, int vectorSizeBytes,
            int numHash, int hashType) {
        super();
        this.bloomCreatedInMap = bloomCreatedInMap;
        this.vectorSizeBytes = vectorSizeBytes;
        this.numHash = numHash;
        this.hashType = hashType;
    }

    public void setBloomKeyType(byte keyType) {
        bloomKeyType = keyType;
    }

    public void setCombiner(boolean isCombiner) {
        this.isCombiner = isCombiner;
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
            if (bloomCreatedInMap) {
                if (bags == null) {
                    return new Result(POStatus.STATUS_EOP, null);
                }
                // Same function for combiner and reducer
                return combineBloomFilters();
            } else {
                if (isCombiner) {
                    return getDistinctBloomKeys();
                } else {
                    if (bags == null) {
                        return new Result(POStatus.STATUS_EOP, null);
                    }
                    return createBloomFilter();
                }
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
        BloomFilter bloomFilter = BuildBloomBase.bloomIn(bloomBytes);
        while (iter.hasNext()) {
            tup = iter.next();
            bloomFilter.or(BuildBloomBase.bloomIn((DataByteArray) tup.get(0)));
        }

        Object partition = key;
        detachInput(); // Free up the key and bags reference

        return getSerializedBloomFilter(partition, bloomFilter, bloomBytes.get().length);
    }

    private Result createBloomFilter() throws IOException {
        // We get a bag of keys. Create a bloom filter from them
        // First do distinct of the keys. Not using DistinctBag as memory should not be a problem.
        HashSet<Object> bloomKeys = new HashSet<>();
        Iterator<Tuple> iter = bags[0].iterator();
        while (iter.hasNext()) {
            bloomKeys.add(iter.next().get(0));
        }

        Object partition = key;
        detachInput(); // Free up the key and bags reference

        BloomFilter bloomFilter = new BloomFilter(vectorSizeBytes * 8, numHash, hashType);
        for (Object bloomKey: bloomKeys) {
            Key k = new Key(DataType.toBytes(bloomKey, bloomKeyType));
            bloomFilter.add(k);
        }
        bloomKeys = null;
        return getSerializedBloomFilter(partition, bloomFilter, vectorSizeBytes + 64);

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

    private Result getDistinctBloomKeys() throws ExecException {
        if (distinctKeyIter == null) {
            HashSet<Object> bloomKeys = new HashSet<>();
            Iterator<Tuple> iter = bags[0].iterator();
            while (iter.hasNext()) {
                bloomKeys.add(iter.next().get(0));
            }
            distinctKeyIter = bloomKeys.iterator();
        }
        while (distinctKeyIter.hasNext()) {
            Tuple res = mTupleFactory.newTuple(2);
            res.set(0, key);
            res.set(1, distinctKeyIter.next());

            Result r = new Result();
            r.result = res;
            r.returnStatus = POStatus.STATUS_OK;
            return r;
        }
        distinctKeyIter = null;
        return new Result(POStatus.STATUS_EOP, null);
    }
}
