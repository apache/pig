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
import java.util.Iterator;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Build a bloom filter for use later in Bloom.  This UDF is intended to run
 * in a group all job.  For example:
 * define bb BuildBloom('jenkins', '100', '0.1');
 * A = load 'foo' as (x, y);
 * B = group A all;
 * C = foreach B generate BuildBloom(A.x);
 * store C into 'mybloom';
 * The bloom filter can be on multiple keys by passing more than one field
 * (or the entire bag) to BuildBloom.
 * The resulting file can then be used in a Bloom filter as:
 * define bloom Bloom(mybloom);
 * A = load 'foo' as (x, y);
 * B = load 'bar' as (z);
 * C = filter B by Bloom(z);
 * D = join C by z, A by x;
 * It uses {@link org.apache.hadoop.util.bloom.BloomFilter}.
 */
public class BuildBloom extends BuildBloomBase<DataByteArray> implements Algebraic {

    /** 
     * Build a bloom filter of fixed size and number of hash functions.
     * @param hashType type of the hashing function (see
     * {@link org.apache.hadoop.util.hash.Hash}).
     * @param mode Will be ignored, though by convention it should be
     * "fixed" or "fixedsize"
     * @param vectorSize The vector size of this filter.
     * @param nbHash The number of hash functions to consider.
     */
    public BuildBloom(String hashType,
                      String mode,
                      String vectorSize,
                      String nbHash) {
        super(hashType, mode, vectorSize, nbHash);
    }

    /** 
     * Construct a Bloom filter based on expected number of elements and
     * desired accuracy.
     * @param hashType type of the hashing function (see
     * {@link org.apache.hadoop.util.hash.Hash}).
     * @param numElements The number of distinct elements expected to be
     * placed in this filter.
     * @param desiredFalsePositive the acceptable rate of false positives.
     * This should be a floating point value between 0 and 1.0, where 1.0
     * would be 100% (ie, a totally useless filter).
     */
    public BuildBloom(String hashType,
                      String numElements,
                      String desiredFalsePositive) {
        super(hashType, numElements, desiredFalsePositive);
    }

    @Override
    public DataByteArray exec(Tuple input) throws IOException {
        throw new IOException("This must be used with algebraic!");
    }

    public String getInitial() {
        return Initial.class.getName();
    }

    public String getIntermed() {
        return Intermediate.class.getName();
    }

    public String getFinal() {
        return Final.class.getName();
    }

    static public class Initial extends BuildBloomBase<Tuple> {

        public Initial() {
        }

        public Initial(String hashType,
                       String mode,
                       String vectorSize,
                       String nbHash ) {
            super(hashType, mode, vectorSize, nbHash);
        }

        public Initial(String hashType,
                       String numElements,
                       String desiredFalsePositive) {
            super(hashType, numElements, desiredFalsePositive);
        }

        @Override
        public Tuple exec(Tuple input) throws IOException {
            if (input == null || input.size() == 0) return null;

            // Strip off the initial level of bag
            DataBag values = (DataBag)input.get(0);
            Iterator<Tuple> it = values.iterator();
            Tuple t = it.next();

            // If the input tuple has only one field, then we'll extract
            // that field and serialize it into a key.  If it has multiple
            // fields, we'll serialize the whole tuple.
            byte[] b;
            if (t.size() == 1) b = DataType.toBytes(t.get(0));
            else b = DataType.toBytes(t, DataType.TUPLE);

            Key k = new Key(b);
            filter = new BloomFilter(vSize, numHash, hType);
            filter.add(k);

            return TupleFactory.getInstance().newTuple(bloomOut());
        }
    }

    static public class Intermediate extends BuildBloomBase<Tuple> {

        public Intermediate() {
        }

        public Intermediate(String hashType,
                            String mode,
                            String vectorSize,
                            String nbHash ) {
            super(hashType, mode, vectorSize, nbHash);
        }

        public Intermediate(String hashType,
                            String numElements,
                            String desiredFalsePositive) {
            super(hashType, numElements, desiredFalsePositive);
        }


        @Override
        public Tuple exec(Tuple input) throws IOException {
            return TupleFactory.getInstance().newTuple(bloomOr(input));
        }
    }

    static public class Final extends BuildBloomBase<DataByteArray> {

        public Final() {
        }

        public Final(String hashType,
                     String mode,
                     String vectorSize,
                     String nbHash ) {
            super(hashType, mode, vectorSize, nbHash);
        }

        public Final(String hashType,
                     String numElements,
                     String desiredFalsePositive) {
            super(hashType, numElements, desiredFalsePositive);
        }

        @Override
        public DataByteArray exec(Tuple input) throws IOException {
            return bloomOr(input);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BYTEARRAY)); 
    }

}
