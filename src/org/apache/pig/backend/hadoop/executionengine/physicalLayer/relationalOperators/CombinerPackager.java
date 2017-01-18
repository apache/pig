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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.util.Arrays;
import java.util.Map;

import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.InternalCachedBag;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.Pair;
/**
 * The package operator that packages the globally rearranged tuples into
 * output format after the combiner stage.  It differs from POPackage in that
 * it does not use the index in the NullableTuple to find the bag to put a
 * tuple in.  Instead, the inputs are put in a bag corresponding to their
 * offset in the tuple.
 */
public class CombinerPackager extends Packager {

    private static final long serialVersionUID = 1L;

    private boolean[] mBags; // For each field, indicates whether or not it
                             // needs to be put in a bag.

    private Map<Integer, Integer> keyLookup;

    private int numBags;

    private transient boolean initialized;
    private transient boolean useDefaultBag;

    /**
     * A new POPostCombinePackage will be constructed as a near clone of the
     * provided POPackage.
     * @param pkg POPackage to clone.
     * @param bags for each field, indicates whether it should be a bag (true)
     * or a simple field (false).
     */
    public CombinerPackager(Packager pkg, boolean[] bags) {
        super();
        keyType = pkg.keyType;
        numInputs = 1;
        inner = new boolean[pkg.inner.length];
        for (int i = 0; i < pkg.inner.length; i++) {
            inner[i] = true;
        }
        if (bags != null) {
            mBags = Arrays.copyOf(bags, bags.length);
        }
        numBags = 0;
        for (int i = 0; i < mBags.length; i++) {
            if (mBags[i]) numBags++;
        }
    }

    @Override
    public void attachInput(Object key, DataBag[] bags, boolean[] readOnce)
            throws ExecException {
        this.key = key;
        this.bags = bags;
        this.readOnce = readOnce;
        // Bag can be read directly and need not be materialized again
    }

    /**
     * @param keyInfo the keyInfo to set
     */
    @Override
    public void setKeyInfo(Map<Integer, Pair<Boolean, Map<Integer, Integer>>> keyInfo) {
        this.keyInfo = keyInfo;
        // TODO: IMPORTANT ASSUMPTION: Currently we only combine in the
        // group case and not in cogroups. So there should only
        // be one LocalRearrange from which we get the keyInfo for
        // which field in the value is in the key. This LocalRearrange
        // has an index of 0. When we do support combiner in Cogroups
        // THIS WILL NEED TO BE REVISITED.
        Pair<Boolean, Map<Integer, Integer>> lrKeyInfo =
                keyInfo.get(0); // assumption: only group are "combinable", hence index 0
        keyLookup = lrKeyInfo.second;
    }

    private DataBag createDataBag(int numBags) {
        if (!initialized) {
            initialized = true;
            if (PigMapReduce.sJobConfInternal.get() != null) {
                String bagType = PigMapReduce.sJobConfInternal.get().get(PigConfiguration.PIG_CACHEDBAG_TYPE);
                if (bagType != null && bagType.equalsIgnoreCase("default")) {
                    useDefaultBag = true;
                }
            }
        }
        return useDefaultBag ? new NonSpillableDataBag() : new InternalCachedBag(numBags);
    }

    @Override
    public Result getNext() throws ExecException {
        if (bags == null) {
            return new Result(POStatus.STATUS_EOP, null);
        }

        // Create numInputs bags
        Object[] fields = new Object[mBags.length];
        for (int i = 0; i < mBags.length; i++) {
            if (mBags[i]) fields[i] = createDataBag(numBags);
        }

        // For each indexed tup in the inp, split them up and place their
        // fields into the proper bags.  If the given field isn't a bag, just
        // set the value as is.
        for (Tuple tup : bags[0]) {
            int tupIndex = 0; // an index for accessing elements from
                              // the value (tup) that we have currently
            for(int i = 0; i < mBags.length; i++) {
                Integer keyIndex = keyLookup.get(i);
                if(keyIndex == null && mBags[i]) {
                    // the field for this index is not the
                    // key - so just take it from the "value"
                    // we were handed - Currently THIS HAS TO BE A BAG
                    // In future if this changes, THIS WILL NEED TO BE
                    // REVISITED.
                    ((DataBag)fields[i]).add((Tuple)tup.get(tupIndex));
                    tupIndex++;
                } else {
                    // the field for this index is in the key
                    fields[i] = key;
                }
            }
        }

        detachInput();

        // The successor of the POPackage(Combiner) as of
        // now SHOULD be a POForeach which has been adjusted
        // to look for its inputs by projecting from the corresponding
        // positions in the POPackage(Combiner) output.
        // So we will NOT be adding the key in the result here but merely
        // putting all bags into a result tuple and returning it.
        Tuple res;
        res = mTupleFactory.newTuple(mBags.length);
        for (int i = 0; i < mBags.length; i++) res.set(i, fields[i]);
        Result r = new Result();
        r.result = res;
        r.returnStatus = POStatus.STATUS_OK;
        return r;

    }

    @Override
    public Tuple getValueTuple(PigNullableWritable keyWritable,
            NullableTuple ntup, int index) throws ExecException {
        return (Tuple) ntup.getValueAsPigType();
    }

}
