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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.InternalCachedBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.pen.Illustrable;
import org.apache.pig.pen.Illustrator;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;

public class Packager implements Illustrable, Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    protected boolean[] readOnce;

    protected DataBag[] bags;

    public static enum PackageType {
        GROUP, JOIN
    };

    protected transient Illustrator illustrator = null;

    // The key being worked on
    Object key;

    // marker to indicate if key is a tuple
    protected boolean isKeyTuple = false;
    // marker to indicate if the tuple key is compound in nature
    protected boolean isKeyCompound = false;

    // key's type
    byte keyType;

    // The number of inputs to this
    // co-group. 0 indicates a distinct, which means there will only be a
    // key, no value.
    int numInputs;

    // If the attaching map-reduce plan use secondary sort key
    boolean useSecondaryKey = false;

    // Denotes if inner is specified
    // on a particular input
    boolean[] inner;

    // flag to denote whether there is a distinct
    // leading to this package
    protected boolean distinct = false;

    // A mapping of input index to key information got from LORearrange
    // for that index. The Key information is a pair of boolean, Map.
    // The boolean indicates whether there is a lone project(*) in the
    // cogroup by. If not, the Map has a mapping of column numbers in the
    // "value" to column numbers in the "key" which contain the fields in
    // the "value"
    protected Map<Integer, Pair<Boolean, Map<Integer, Integer>>> keyInfo;

    private PackageType pkgType;

    boolean firstTime = true;
    boolean useDefaultBag = false;

    protected POPackage parent = null;

    protected static final BagFactory mBagFactory = BagFactory.getInstance();
    protected static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    public Object getKey(PigNullableWritable key) throws ExecException {
        Object keyObject = key.getValueAsPigType();
        if (useSecondaryKey) {
            return ((Tuple) keyObject).get(0);
        } else {
            return keyObject;
        }
    }

    public void attachInput(Object key, DataBag[] bags, boolean[] readOnce)
            throws ExecException {
        checkBagType();

        this.key = key;
        this.bags = bags;
        this.readOnce = readOnce;
        // We assume that we need all bags materialized. Specialized subclasses
        // may choose to handle this differently
        for (int i = 0; i < bags.length; i++) {
            if (readOnce[i]) {
                DataBag materializedBag = getBag();
                materializedBag.addAll(bags[i]);
                bags[i] = materializedBag;
            }
        }
    }

    public Result getNext() throws ExecException {
        if (bags == null) {
            return new Result(POStatus.STATUS_EOP, null);
        }
        Tuple res;

        if (isDistinct()) {
            // only set the key which has the whole
            // tuple
            res = mTupleFactory.newTuple(1);
            res.set(0, key);
        } else {
            // Construct the output tuple by appending
            // the key and all the above constructed bags
            // and return it.
            res = mTupleFactory.newTuple(numInputs + 1);
            res.set(0, key);
            int i = -1;
            for (DataBag bag : bags) {
                i++;
                if (inner[i]) {
                    if (bag.size() == 0) {
                        detachInput();
                        Result r = new Result();
                        r.returnStatus = POStatus.STATUS_NULL;
                        return r;
                    }
                }

                res.set(i + 1, bag);
            }
        }
        Result r = new Result();
        r.returnStatus = POStatus.STATUS_OK;
        r.result = illustratorMarkup(null, res, 0);
        detachInput();
        return r;
    }

    public void detachInput() {
        key = null;
        bags = null;
    }

    protected Tuple illustratorMarkup2(Object in, Object out) {
        if (illustrator != null) {
            ExampleTuple tOut;
            if (!(out instanceof ExampleTuple)) {
                tOut = new ExampleTuple((Tuple) out);
            } else {
                tOut = (ExampleTuple) out;
            }
            illustrator.getLineage().insert(tOut);
            tOut.synthetic = ((ExampleTuple) in).synthetic;
            illustrator.getLineage().union(tOut, (Tuple) in);
            return tOut;
        } else
            return (Tuple) out;
    }

    protected Tuple starMarkup(Tuple key, Tuple val, Tuple out){
        if (illustrator != null){
            Tuple copy = illustratorMarkup2(key, out);
            // For distinct, we also need to retain lineage information from the values.
            if (isDistinct())
                copy = illustratorMarkup2(val, out);
            return copy;
        } else
            return (Tuple) out;
    }

    public Tuple getValueTuple(PigNullableWritable keyWritable,
            NullableTuple ntup, int index) throws ExecException {
        Object key = getKey(keyWritable);
        // Need to make a copy of the value, as hadoop uses the same ntup
        // to represent each value.
        Tuple val = (Tuple) ntup.getValueAsPigType();

        Tuple copy = null;
        // The "value (val)" that we just got may not
        // be the complete "value". It may have some portions
        // in the "key" (look in POLocalRearrange for more comments)
        // If this is the case we need to stitch
        // the "value" together.
        Pair<Boolean, Map<Integer, Integer>> lrKeyInfo = keyInfo.get(index);
        boolean isProjectStar = lrKeyInfo.first;
        Map<Integer, Integer> keyLookup = lrKeyInfo.second;
        int keyLookupSize = keyLookup.size();

        if (keyLookupSize > 0) {

            // we have some fields of the "value" in the
            // "key".
            int finalValueSize = keyLookupSize + val.size();
            copy = mTupleFactory.newTuple(finalValueSize);
            int valIndex = 0; // an index for accessing elements from
                              // the value (val) that we have currently
            for (int i = 0; i < finalValueSize; i++) {
                Integer keyIndex = keyLookup.get(i);
                if (keyIndex == null) {
                    // the field for this index is not in the
                    // key - so just take it from the "value"
                    // we were handed
                    copy.set(i, val.get(valIndex));
                    valIndex++;
                } else {
                    // the field for this index is in the key
                    if (isKeyTuple && isKeyCompound) {
                        // the key is a tuple, extract the
                        // field out of the tuple
                        copy.set(i, ((Tuple) key).get(keyIndex));
                    } else {
                        copy.set(i, key);
                    }
                }
            }
            copy = illustratorMarkup2(val, copy);
        } else if (isProjectStar) {

            // the whole "value" is present in the "key"
            copy = mTupleFactory.newTuple(((Tuple) key).getAll());
            copy = starMarkup((Tuple) key, val, copy);
        } else {

            // there is no field of the "value" in the
            // "key" - so just make a copy of what we got
            // as the "value"
            copy = mTupleFactory.newTuple(val.getAll());
            copy = illustratorMarkup2(val, copy);
        }
        return copy;
    }

    public byte getKeyType() {
        return keyType;
    }

    public void setKeyType(byte keyType) {
        this.keyType = keyType;
    }

    /**
     * @return the isKeyTuple
     */
    public boolean getKeyTuple() {
        return isKeyTuple;
    }

    /**
     * @return the keyAsTuple
     */
    public Tuple getKeyAsTuple() {
        return isKeyTuple ? (Tuple) key : null;
    }

    /**
     * @return the key
     */
    public Object getKey() {
        return key;
    }

    public boolean[] getInner() {
        return inner;
    }

    public void setInner(boolean[] inner) {
        this.inner = inner;
    }

    /**
     * @param keyInfo the keyInfo to set
     */
    public void setKeyInfo(
            Map<Integer, Pair<Boolean, Map<Integer, Integer>>> keyInfo) {
        this.keyInfo = keyInfo;
    }

    /**
     * @param keyTuple the keyTuple to set
     */
    public void setKeyTuple(boolean keyTuple) {
        this.isKeyTuple = keyTuple;
    }

    /**
     * @param keyCompound the keyCompound to set
     */
    public void setKeyCompound(boolean keyCompound) {
        this.isKeyCompound = keyCompound;
    }

    /**
     * @return the keyInfo
     */
    public Map<Integer, Pair<Boolean, Map<Integer, Integer>>> getKeyInfo() {
        return keyInfo;
    }

    public Illustrator getIllustrator() {
        return illustrator;
    }

    @Override
    public void setIllustrator(Illustrator illustrator) {
        this.illustrator = illustrator;
    }

    /**
     * @return the distinct
     */
    public boolean isDistinct() {
        return distinct;
    }

    /**
     * @param distinct the distinct to set
     */
    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public void setUseSecondaryKey(boolean useSecondaryKey) {
        this.useSecondaryKey = useSecondaryKey;
    }

    public void setPackageType(PackageType type) {
        this.pkgType = type;
    }

    public PackageType getPackageType() {
        return pkgType;
    }

    public int getNumInputs(byte index) {
        return numInputs;
    }

    public int getNumInputs() {
        return numInputs;
    }

    public void setNumInputs(int numInputs) {
        this.numInputs = numInputs;
    }

    @Override
    public Packager clone() throws CloneNotSupportedException {
        Packager clone = (Packager) super.clone();
        clone.setNumInputs(numInputs);
        clone.setPackageType(pkgType);
        clone.setDistinct(distinct);
        if (inner != null) {
            clone.inner = new boolean[inner.length];
            for (int i = 0; i < inner.length; i++) {
                clone.inner[i] = inner[i];
            }
        } else
            clone.inner = null;
        if (keyInfo != null)
            clone.setKeyInfo(new HashMap<Integer, Pair<Boolean, Map<Integer, Integer>>>(
                    keyInfo));
        clone.setKeyCompound(isKeyCompound);
        clone.setKeyTuple(isKeyTuple);
        clone.setKeyType(keyType);
        clone.setUseSecondaryKey(useSecondaryKey);
        return clone;
    }

    public String name() {
        return this.getClass().getSimpleName();
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        // All customized packagers are introduced during MRCompilaition.
        // Illustrate happens before that, so we only have to focus on the basic
        // POPackage
        if (illustrator != null) {
            ExampleTuple tOut = new ExampleTuple((Tuple) out);
            LineageTracer lineageTracer = illustrator.getLineage();
            lineageTracer.insert(tOut);
            boolean synthetic = false;
            if (illustrator.getEquivalenceClasses() == null) {
                LinkedList<IdentityHashSet<Tuple>> equivalenceClasses = new LinkedList<IdentityHashSet<Tuple>>();
                for (int i = 0; i < numInputs; ++i) {
                    IdentityHashSet<Tuple> equivalenceClass = new IdentityHashSet<Tuple>();
                    equivalenceClasses.add(equivalenceClass);
                }
                illustrator.setEquivalenceClasses(equivalenceClasses, parent);
            }

            if (isDistinct()) {
                int count = 0;
                for (Tuple tmp : bags[0]){
                    count++;
                    if (!tmp.equals(tOut))
                        lineageTracer.union(tOut, tmp);
                }
                if (count > 1) // only non-distinct tuples are inserted into the
                    // equivalence class
                    illustrator.getEquivalenceClasses().get(eqClassIndex)
                    .add(tOut);
                illustrator.addData((Tuple) tOut);
                return (Tuple) tOut;
            }
            boolean outInEqClass = true;
            try {
                for (int i = 1; i < numInputs + 1; i++) {
                    DataBag dbs = (DataBag) ((Tuple) out).get(i);
                    Iterator<Tuple> iter = dbs.iterator();
                    if (dbs.size() <= 1 && outInEqClass) // all inputs have >= 2
                        // records
                        outInEqClass = false;
                    while (iter.hasNext()) {
                        Tuple tmp = iter.next();
                        // any of synthetic data in bags causes the output tuple
                        // to be synthetic
                        if (!synthetic && ((ExampleTuple) tmp).synthetic)
                            synthetic = true;
                        lineageTracer.union(tOut, tmp);
                    }
                }
            } catch (ExecException e) {
                // TODO better exception handling
                throw new RuntimeException("Illustrator exception :"
                        + e.getMessage());
            }
            if (outInEqClass)
                illustrator.getEquivalenceClasses().get(eqClassIndex).add(tOut);
            tOut.synthetic = synthetic;
            illustrator.addData((Tuple) tOut);
            return tOut;
        } else
            return (Tuple) out;
    }

    public void setParent(POPackage pack) {
        parent = pack;
    }

    public int numberOfEquivalenceClasses() {
        return 1;
    }

    public void checkBagType() {
        if(firstTime){
            firstTime = false;
            if (PigMapReduce.sJobConfInternal.get() != null) {
                String bagType = PigMapReduce.sJobConfInternal.get().get("pig.cachedbag.type");
                if (bagType != null && bagType.equalsIgnoreCase("default")) {
                    useDefaultBag = true;
                }
            }
        }
    }

    public DataBag getBag(){
        return useDefaultBag ? mBagFactory.newDefaultBag()
                // In a very rare case if there is a POStream after this
                // POJoinPackage in the pipeline and is also blocking the pipeline;
                // constructor argument should be 2 * numInputs. But for one obscure
                // case we don't want to pay the penalty all the time.
                : new InternalCachedBag(numInputs);
    }
}
