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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.NullableUnknownWritable;
import org.apache.pig.impl.io.PigNullableWritable;

/**
 * The package operator that packages the globally rearranged tuples
 * into output format as required by multi-query de-multiplexer.
 * <p>
 * This operator is used when merging multiple Map-Reduce splittees
 * into a Map-only splitter during multi-query optimization.
 * The package operators of the reduce plans of the splittees form an
 * indexed package list inside this operator. When this operator
 * receives an input, it extracts the index from the key and calls the
 * corresponding package to get the output data.
 * <p>
 * Due to the recursive nature of multi-query optimization, this operator
 * may be contained in another multi-query packager.
 * <p>
 * The successor of this operator must be a PODemux operator which
 * knows how to consume the output of this operator.
 */
public class MultiQueryPackager extends Packager {

    private static final long serialVersionUID = 1L;

    private static int idxPart = 0x7F;

    private List<Packager> packagers = new ArrayList<Packager>();

    /**
     * If the POLocalRearranges corresponding to the reduce plans in
     * myPlans (the list of inner plans of the demux) have different key types
     * then the MultiQueryOptimizer converts all the keys to be of type tuple
     * by wrapping any non-tuple keys into Tuples (keys which are already tuples
     * are left alone).
     * The list below is a list of booleans indicating whether extra tuple wrapping
     * was done for the key in the corresponding POLocalRearranges and if we need
     * to "unwrap" the tuple to get to the key
     */
    private ArrayList<Boolean> isKeyWrapped = new ArrayList<Boolean>();

    /*
     * Indicating if all the inner plans have the same
     * map key type. If not, the keys passed in are
     * wrapped inside tuples and need to be extracted
     * out during the reduce phase
     */
    private boolean sameMapKeyType = true;

    /*
     * Indicating if this operator is in a combiner.
     * If not, this operator is in a reducer and the key
     * values must first be extracted from the tuple-wrap
     * before writing out to the disk
     */
    private boolean inCombiner = false;

    private PigNullableWritable keyWritable = null;

    /**
     * Appends the specified package object to the end of
     * the package list.
     * 
     * @param pack package to be appended to the list
     */
    public void addPackager(Packager pkgr) {
        packagers.add(pkgr);
    }

    /**
     * Appends the specified package object to the end of
     * the package list.
     * 
     * @param pack package to be appended to the list
     * @param mapKeyType the map key type associated with the package
     */
    public void addPackager(Packager pkgr, byte mapKeyType) {
        packagers.add(pkgr);
        // if mapKeyType is already a tuple, we will NOT
        // be wrapping it in an extra tuple. If it is not
        // a tuple, we will wrap into in a tuple
        isKeyWrapped.add(mapKeyType == DataType.TUPLE ? false : true);
    }

    /**
     * Returns the list of packages.
     * 
     * @return the list of the packages
     */
    public List<Packager> getPackagers() {
        return packagers;
    }

    /**
     * Constructs the output tuple from the inputs.
     * <p>
     * The output is consumed by for the demultiplexer operator
     * (PODemux) in the format (key, {bag of tuples}) where key
     * is an indexed WritableComparable, not the wrapped value as a pig type.
     */
    @Override
    public Result getNext() throws ExecException {
        if (bags == null) {
            return new Result(POStatus.STATUS_EOP, null);
        }

        byte origIndex = keyWritable.getIndex();

        int index = (int)origIndex;
        index &= idxPart;

        if (index >= packagers.size() || index < 0) {
            int errCode = 2140;
            String msg = "Invalid package index " + index
                    + " should be in the range between 0 and "
                    + packagers.size();
            throw new ExecException(msg, errCode, PigException.BUG);
        }

        Packager pkgr = packagers.get(index);

        // check to see if we need to unwrap the key. The keys may be
        // wrapped inside a tuple by LocalRearrange operator when jobs
        // with different map key types are merged
        PigNullableWritable curKey = keyWritable;
        if (!sameMapKeyType && !inCombiner && isKeyWrapped.get(index)) {
            Tuple tup = (Tuple) keyWritable.getValueAsPigType();
            curKey = HDataType.getWritableComparableTypes(tup.get(0),
                    pkgr.getKeyType());
            curKey.setIndex(origIndex);
        }

        pkgr.attachInput(curKey, bags, readOnce);

        Result res = pkgr.getNext();
        pkgr.detachInput();
        detachInput();

        Tuple tuple = (Tuple)res.result;

        // the object present in the first field
        // of the tuple above is the real data without
        // index information - this is because the
        // package above, extracts the real data out of
        // the PigNullableWritable object - we are going to
        // give this result tuple to a PODemux operator
        // which needs a PigNullableWritable first field so
        // it can figure out the index. Therefore we need
        // to add index to the first field of the tuple.

        Object obj = tuple.get(0);
        if (obj instanceof PigNullableWritable) {
            ((PigNullableWritable)obj).setIndex(origIndex);
        }
        else {
            PigNullableWritable myObj = null;
            if (obj == null) {
                myObj = new NullableUnknownWritable();
                myObj.setNull(true);
            }
            else {
                myObj = HDataType.getWritableComparableTypes(obj, HDataType.findTypeFromNullableWritable(curKey));
            }
            myObj.setIndex(origIndex);
            tuple.set(0, myObj);
        }
        // illustrator markup has been handled by "pkgr"
        return res;
    }

    /**
     * Returns the list of booleans that indicates if the
     * key needs to unwrapped for the corresponding plan.
     * 
     * @return the list of isKeyWrapped boolean values
     */
    public List<Boolean> getIsKeyWrappedList() {
        return Collections.unmodifiableList(isKeyWrapped);
    }

    /**
     * Adds a list of IsKeyWrapped boolean values
     * 
     * @param lst the list of boolean values to add
     */
    public void addIsKeyWrappedList(List<Boolean> lst) {
        for (Boolean b : lst) {
            isKeyWrapped.add(b);
        }
    }

    public void setInCombiner(boolean inCombiner) {
        this.inCombiner = inCombiner;
    }

    public boolean isInCombiner() {
        return inCombiner;
    }

    public void setSameMapKeyType(boolean sameMapKeyType) {
        this.sameMapKeyType = sameMapKeyType;
    }

    public boolean isSameMapKeyType() {
        return sameMapKeyType;
    }

    @Override
    public int getNumInputs(byte index) {
        return packagers.get(((int) index) & idxPart).getNumInputs(index);
    }

    @Override
    public Tuple getValueTuple(PigNullableWritable keyWritable,
            NullableTuple ntup, int origIndex) throws ExecException {
        this.keyWritable = keyWritable;
        int index = origIndex & idxPart;
        PigNullableWritable newKey = keyWritable;
        if (!sameMapKeyType && !inCombiner && isKeyWrapped.get(index)) {                                       
            Tuple tup = (Tuple)this.keyWritable.getValueAsPigType();
            newKey = HDataType.getWritableComparableTypes(tup.get(0), packagers.get(index).getKeyType());
            newKey.setIndex((byte)origIndex);
        }
        return packagers.get(index).getValueTuple(newKey, ntup, origIndex);
    }
}
