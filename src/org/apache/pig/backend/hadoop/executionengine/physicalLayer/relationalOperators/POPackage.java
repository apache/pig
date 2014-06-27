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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.AccumulativeBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.InternalCachedBag;
import org.apache.pig.data.ReadOnceBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.Illustrator;

/**
 * The package operator that packages
 * the globally rearranged tuples into
 * output format as required by co-group.
 * This is last stage of processing co-group.
 * This operator has a slightly different
 * format than other operators in that, it
 * takes two things as input. The key being
 * worked on and the iterator of bags that
 * contain indexed tuples that just need to
 * be packaged into their appropriate output
 * bags based on the index.
 */
public class POPackage extends PhysicalOperator {

    private static final long serialVersionUID = 1L;

    //The iterator of indexed Tuples
    //that is typically provided by
    //Hadoop
    transient Iterator<NullableTuple> tupIter;

    //The key being worked on
    protected Object key;

    //The number of inputs to this
    //co-group.  0 indicates a distinct, which means there will only be a
    //key, no value.
    protected int numInputs;

    protected static final BagFactory mBagFactory = BagFactory.getInstance();
    protected static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    private boolean firstTime = true;

    private boolean useDefaultBag = false;

    private boolean lastBagReadOnly = true;

    protected Packager pkgr;

    protected PigNullableWritable keyWritable;

    public POPackage(OperatorKey k) {
        this(k, -1, null);
    }

    public POPackage(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POPackage(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    public POPackage(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        this(k, rp, inp, new Packager());
    }

    public POPackage(OperatorKey k, int rp, List<PhysicalOperator> inp,
            Packager pkgr) {
        super(k, rp, inp);
        numInputs = -1;
        this.pkgr = pkgr;
    }

    public POPackage(POPackage copy) {
        super(copy);
        this.numInputs = copy.numInputs;
        this.pkgr = copy.pkgr;
        this.pkgr.keyInfo = copy.pkgr.keyInfo;
    }

    @Override
    public void setIllustrator(Illustrator illustrator) {
        super.setIllustrator(illustrator);
        pkgr.setIllustrator(illustrator);
    }

    @Override
    public String name() {
        return getAliasString() + "Package" + "(" + pkgr.name() + ")" + "["
                + DataType.findTypeName(resultType) + "]" + "{"
                + DataType.findTypeName(pkgr.getKeyType()) + "}" + " - "
                + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitPackage(this);
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    /**
     * Attaches the required inputs
     * @param k - the key being worked on
     * @param inp - iterator of indexed tuples typically
     *              obtained from Hadoop
     */
    public void attachInput(PigNullableWritable k, Iterator<NullableTuple> inp) {
        try {
            tupIter = inp;
            key = pkgr.getKey(k);
            keyWritable = k;
            inputAttached = true;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Error attaching input for key " + k +
                    " in " + name() + " at location " + getOriginalLocations(), e);
        }
    }

    /**
     * attachInput's better half!
     */
    @Override
    public void detachInput() {
        tupIter = null;
        key = null;
        inputAttached = false;
    }

    public int getNumInps() {
        return numInputs;
    }

    public void setNumInps(int numInps) {
        this.numInputs = numInps;
        pkgr.setNumInputs(numInps);
    }

    /**
     * From the inputs, constructs the output tuple for this co-group in the
     * required format which is (key, {bag of tuples from input 1}, {bag of
     * tuples from input 2}, ...)
     */
    @Override
    public Result getNextTuple() throws ExecException {
        if(firstTime){
            firstTime = false;
            if (PigMapReduce.sJobConfInternal.get() != null) {
                String bagType = PigMapReduce.sJobConfInternal.get().get(
                        "pig.cachedbag.type");
                if (bagType != null && bagType.equalsIgnoreCase("default")) {
                    useDefaultBag = true;
                }
            }
            // If multiquery, the last bag is InternalCachedBag and should not
            // set ReadOnly flag, otherwise we will materialize again to another
            // InternalCachedBag
            if (pkgr instanceof MultiQueryPackager) {
                lastBagReadOnly = false;
            }
        }
        int numInputs = pkgr.getNumInputs(keyWritable.getIndex());
        boolean[] readOnce = new boolean[numInputs];
        for (int i = 0; i < numInputs; i++)
            readOnce[i] = false;

        if (isInputAttached()) {
            // Create numInputs bags
            DataBag[] dbs = null;
            dbs = new DataBag[numInputs];

            if (isAccumulative()) {
                readOnce[numInputs - 1] = false;
                // create bag wrapper to pull tuples in many batches
                // all bags have reference to the sample tuples buffer
                // which contains tuples from one batch
                POPackageTupleBuffer buffer = new POPackageTupleBuffer();
                buffer.setKey(key);
                buffer.setIterator(tupIter);
                for (int i = 0; i < numInputs; i++) {
                    dbs[i] = new AccumulativeBag(buffer, i);
                }

            } else {
                if (lastBagReadOnly) {
                    readOnce[numInputs - 1] = true;
                }

                // We know the tuples will come sorted by index, so we can wrap
                // the last input in a ReadOnceBag and let the Packager decide
                // whether or not to read into memory

                // create bag to pull all tuples out of iterator
                for (int i = 0; i < numInputs; i++) {
                    dbs[i] = useDefaultBag ? BagFactory.getInstance()
                            .newDefaultBag()
                            // In a very rare case if there is a POStream after this
                            // POPackage in the pipeline and is also blocking the
                            // pipeline;
                            // constructor argument should be 2 * numInputs. But for one
                            // obscure
                            // case we don't want to pay the penalty all the time.
                            : new InternalCachedBag(numInputs);
                }
                // For each indexed tup in the inp, sort them
                // into their corresponding bags based
                // on the index
                while (tupIter.hasNext()) {
                    NullableTuple ntup = tupIter.next();
                    int index = ntup.getIndex();
                    if (index == numInputs - 1) {
                        dbs[index] = new PeekedBag(pkgr, ntup, tupIter, keyWritable);
                        break;
                    }
                    Tuple copy = pkgr.getValueTuple(keyWritable, ntup, index);

                    if (numInputs == 1) {

                        // this is for multi-query merge where
                        // the numInputs is always 1, but the index
                        // (the position of the inner plan in the
                        // enclosed operator) may not be 1.
                        dbs[0].add(copy);
                    } else {
                        dbs[index].add(copy);
                    }
                    if (getReporter() != null) {
                        getReporter().progress();
                    }
                }
            }
            // Construct the output tuple by appending
            // the key and all the above constructed bags
            // and return it.
            pkgr.attachInput(key, dbs, readOnce);
            detachInput();
        }

        return pkgr.getNext();
    }

    public Packager getPkgr() {
        return pkgr;
    }

    public void setPkgr(Packager pkgr) {
        this.pkgr = pkgr;
        pkgr.setParent(this);
        pkgr.setIllustrator(illustrator);
    }

    /**
     * Make a deep copy of this operator.
     * @throws CloneNotSupportedException
     */
    @Override
    public POPackage clone() throws CloneNotSupportedException {
        POPackage clone = (POPackage)super.clone();
        clone.mKey = new OperatorKey(mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope));
        clone.requestedParallelism = requestedParallelism;
        clone.resultType = resultType;
        clone.numInputs = numInputs;
        clone.pkgr = (Packager) this.pkgr.clone();
        return clone;
    }

    public class POPackageTupleBuffer implements AccumulativeTupleBuffer {
        private List<Tuple>[] bags;
        private Iterator<NullableTuple> iter;
        private int batchSize;
        private Object currKey;

        @SuppressWarnings("unchecked")
        public POPackageTupleBuffer() {
            batchSize = 20000;
            if (PigMapReduce.sJobConfInternal.get() != null) {
                String size = PigMapReduce.sJobConfInternal.get().get("pig.accumulative.batchsize");
                if (size != null) {
                    batchSize = Integer.parseInt(size);
                }
            }

            this.bags = new List[numInputs];
            for(int i=0; i<numInputs; i++) {
                this.bags[i] = new ArrayList<Tuple>();
            }
        }

        public void setKey(Object key) {
            this.currKey = key;
        }

        public void setIterator(Iterator<NullableTuple> iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNextBatch() {
            return iter.hasNext();
        }

        @Override
        public void nextBatch() throws IOException {
            for(int i=0; i<bags.length; i++) {
                bags[i].clear();
            }

            key = currKey;
            for(int i=0; i<batchSize; i++) {
                if (iter.hasNext()) {
                    NullableTuple ntup = iter.next();
                    int index = ntup.getIndex();
                    Tuple copy = pkgr.getValueTuple(keyWritable, ntup, index);
                    if (numInputs == 1) {

                        // this is for multi-query merge where
                        // the numInputs is always 1, but the index
                        // (the position of the inner plan in the
                        // enclosed operator) may not be 1.
                        bags[0].add(copy);
                    } else {
                        bags[index].add(copy);
                    }
                }else{
                    break;
                }
            }
        }

        @Override
        public void clear() {
            for(int i=0; i<bags.length; i++) {
                bags[i].clear();
            }
            iter = null;
        }

        @Override
        public Iterator<Tuple> getTuples(int index) {
            return bags[index].iterator();
        }

        public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
            return POPackage.this.illustratorMarkup(in, out, eqClassIndex);
        }
    };

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        return pkgr.illustratorMarkup(in, out, eqClassIndex);
    }

    public int numberOfEquivalenceClasses() {
        return pkgr.numberOfEquivalenceClasses();
    }

    // A ReadOnceBag that we've already "peeked" at
    private static class PeekedBag extends ReadOnceBag {
        private static final long serialVersionUID = 1L;
        NullableTuple head;
        int index;

        public PeekedBag(Packager pkgr, NullableTuple head,
                Iterator<NullableTuple> tupIter,
                PigNullableWritable keyWritable) {
            super(pkgr, tupIter, keyWritable);
            this.head = head;
            this.index = head.getIndex();
        }

        @Override
        public Iterator<Tuple> iterator() {
            return new Iterator<Tuple>() {
                boolean headReturned = false;

                @Override
                public boolean hasNext() {
                    if (!headReturned)
                        return true;

                    return tupIter.hasNext();
                }

                @Override
                public Tuple next() {
                    if (!headReturned) {
                        headReturned = true;
                        try {
                            return pkgr.getValueTuple(keyWritable, head,
                                    head.getIndex());
                        } catch (ExecException e) {
                            throw new RuntimeException(
                                    "PeekedBag failed to get value tuple : "
                                            + e.toString());
                        }
                    }
                    NullableTuple ntup = tupIter.next();
                    Tuple ret = null;
                    try {
                        ret = pkgr.getValueTuple(keyWritable, ntup, index);
                    } catch (ExecException e) {
                        throw new RuntimeException(
                                "PeekedBag failed to get value tuple : "
                                        + e.toString());
                    }
                    if (getReporter() != null) {
                        getReporter().progress();
                    }
                    return ret;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException(
                            "PeekedBag does not support removal");
                }
            };
        }
    }

}
