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

package org.apache.pig.builtin;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Iterator;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.hash.Hash;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * A Base class for BuildBloom and its Algebraic implementations.
 */
public abstract class BuildBloomBase<T> extends EvalFunc<T> {

    protected int vSize;
    protected int numHash;
    protected int hType;
    protected BloomFilter filter;

    protected BuildBloomBase() {
    }

    /** 
     * @param hashType type of the hashing function (see
     * {@link org.apache.hadoop.util.hash.Hash}).
     * @param mode Will be ignored, though by convention it should be
     * "fixed" or "fixedsize"
     * @param vectorSize The vector size of <i>this</i> filter.
     * @param nbHash The number of hash functions to consider.
     */
    public BuildBloomBase(String hashType,
                          String mode,
                          String vectorSize,
                          String nbHash) {
        vSize = Integer.valueOf(vectorSize);
        numHash = Integer.valueOf(nbHash);
        hType = convertHashType(hashType);
    }

    /** 
     * @param hashType type of the hashing function (see
     * {@link org.apache.hadoop.util.hash.Hash}).
     * @param numElements The number of distinct elements expected to be
     * placed in this filter.
     * @param desiredFalsePositive the acceptable rate of false positives.
     * This should be a floating point value between 0 and 1.0, where 1.0
     * would be 100% (ie, a totally useless filter).
     */
    public BuildBloomBase(String hashType,
                          String numElements,
                          String desiredFalsePositive) {
        setSize(numElements, desiredFalsePositive);
        hType = convertHashType(hashType);
    }


    protected DataByteArray bloomOr(Tuple input) throws IOException {
        filter = new BloomFilter(vSize, numHash, hType);

        try {
            DataBag values = (DataBag)input.get(0);
            for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
                Tuple t = it.next();
                filter.or(bloomIn((DataByteArray)t.get(0)));
            }
        } catch (ExecException ee) {
            throw new IOException(ee);
        }

        return bloomOut();
    }

    protected DataByteArray bloomOut() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(vSize / 8);
        DataOutputStream dos = new DataOutputStream(baos);
        filter.write(dos);
        return new DataByteArray(baos.toByteArray());
    }

    protected BloomFilter bloomIn(DataByteArray b) throws IOException {
        DataInputStream dis = new DataInputStream(new
            ByteArrayInputStream(b.get()));
        BloomFilter f = new BloomFilter();
        f.readFields(dis);
        return f;
    }

    private int convertHashType(String hashType) {
        if (hashType.toLowerCase().contains("jenkins")) {
            return Hash.JENKINS_HASH;
        } else if (hashType.toLowerCase().contains("murmur")) {
            return Hash.MURMUR_HASH;
        } else {
            throw new RuntimeException("Unknown hash type " + hashType +
                ".  Valid values are jenkins and murmur.");
        }
    }

    private void setSize(String numElements, String desiredFalsePositive) {
        int num = Integer.valueOf(numElements);
        float fp = Float.valueOf(desiredFalsePositive);
        if (num < 1 || fp < 0.0 || fp >= 1.0) {
            throw new RuntimeException("Number of elements must be greater "
                + "than zero and desiredFalsePositive must be between 0 "
                + " and 1.");
        }
        vSize = (int)(-1 * (num * Math.log(fp)) / Math.pow(Math.log(2), 2));
        log.info("BuildBloom setting vector size to " + vSize);

        numHash = (int)(0.7 * vSize / num);
        log.info("BuildBloom setting number of hashes to " + numHash);
    }


}
