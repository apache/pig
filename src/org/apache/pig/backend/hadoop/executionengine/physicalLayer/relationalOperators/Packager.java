package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.Pair;

public class Packager implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    protected boolean[] readOnce;

    protected DataBag[] bags;

    public static enum PackageType {
        GROUP, JOIN
    };

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

    protected static final BagFactory mBagFactory = BagFactory.getInstance();
    protected static final TupleFactory mTupleFactory = TupleFactory
            .getInstance();

    Object getKey(Object key) throws ExecException {
        if (useSecondaryKey) {
            return ((Tuple) key).get(0);
        } else {
            return key;
        }
    }

    void attachInput(Object key, DataBag[] bags, boolean[] readOnce)
            throws ExecException {
        this.key = key;
        this.bags = bags;
        this.readOnce = readOnce;
    }

    public Result getNext() throws ExecException {
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

                if (!readOnce[i]) {
                    res.set(i + 1, bag);
                } else {
                    DataBag readBag = mBagFactory.newDefaultBag();
                    readBag.addAll(bag);
                    res.set(i + 1, readBag);
                }
            }
        }
        Result r = new Result();
        r.returnStatus = POStatus.STATUS_OK;
        // if (!isAccumulative())
        // r.result = illustratorMarkup(null, res, 0);
        // else
        r.result = res;
        return r;
    }

    void detachInput() {
        key = null;
        bags = null;
    }

    protected Tuple getValueTuple(Object key, NullableTuple ntup, int index)
            throws ExecException {
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
            // copy = illustratorMarkup2(val, copy);
        } else if (isProjectStar) {

            // the whole "value" is present in the "key"
            copy = mTupleFactory.newTuple(((Tuple) key).getAll());
            // copy = illustratorMarkup2((Tuple) key, copy);
        } else {

            // there is no field of the "value" in the
            // "key" - so just make a copy of what we got
            // as the "value"
            copy = mTupleFactory.newTuple(val.getAll());
            // copy = illustratorMarkup2(val, copy);
        }
        return copy;
    }

    public byte getKeyType() {
        return keyType;
    }

    public void setKeyType(byte keyType) {
        this.keyType = keyType;
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

    public int getNumInputs() {
        return numInputs;
    }

    public void setNumInputs(int numInputs) {
        this.numInputs = numInputs;
    }

    public Packager clone() throws CloneNotSupportedException {
        Packager clone = (Packager) super.clone();
        clone.setNumInputs(numInputs);
        clone.setPackageType(pkgType);
        clone.setDistinct(distinct);
        clone.setInner(inner);
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
}
