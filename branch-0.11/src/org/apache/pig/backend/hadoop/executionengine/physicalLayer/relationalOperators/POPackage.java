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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.AccumulativeBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.InternalCachedBag;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;

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
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    

    public static enum PackageType { GROUP, JOIN };
    
    //The iterator of indexed Tuples
    //that is typically provided by
    //Hadoop
    transient Iterator<NullableTuple> tupIter;

    //The key being worked on
    Object key;
    
    // marker to indicate if key is a tuple
    protected boolean isKeyTuple = false;
    // marker to indicate if the tuple key is compound in nature
    protected boolean isKeyCompound = false;
    // key as a Tuple object (if the key is a tuple)
    protected Tuple keyAsTuple;
    
    //key's type
    byte keyType;

    //The number of inputs to this
    //co-group.  0 indicates a distinct, which means there will only be a
    //key, no value.
    int numInputs;
    
    // If the attaching map-reduce plan use secondary sort key
    boolean useSecondaryKey = false;
    
    //Denotes if inner is specified
    //on a particular input
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

    protected static final BagFactory mBagFactory = BagFactory.getInstance();
    protected static final TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    private boolean firstTime = true;
    
    private boolean useDefaultBag = false;
    
    private PackageType pkgType;

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
        super(k, rp, inp);
        numInputs = -1;
        keyInfo = new HashMap<Integer, Pair<Boolean, Map<Integer, Integer>>>();
    }

    @Override
    public String name() {
        return getAliasString() + "Package" + "["
                + DataType.findTypeName(resultType) + "]" + "{"
                + DataType.findTypeName(keyType) + "}" + " - "
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
        tupIter = inp;
        key = k.getValueAsPigType();
        if (useSecondaryKey)
        {
            try {
                key = ((Tuple)key).get(0);
            } catch (ExecException e) {
                // TODO Exception
                throw new RuntimeException(e);
            }
        }
        if(isKeyTuple) {
            // key is a tuple, cache the key as a
            // tuple for use in the getNext()
            keyAsTuple = (Tuple)key;
        }
    }

    /**
     * attachInput's better half!
     */
    public void detachInput() {
        tupIter = null;
        key = null;
    }

    public int getNumInps() {
        return numInputs;
    }

    public void setNumInps(int numInps) {
        this.numInputs = numInps;
    }
    
    public boolean[] getInner() {
        return inner;
    }

    public void setInner(boolean[] inner) {
        this.inner = inner;
    }
   
    /**
     * From the inputs, constructs the output tuple
     * for this co-group in the required format which
     * is (key, {bag of tuples from input 1}, {bag of tuples from input 2}, ...)
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        Tuple res;
        
        if(firstTime){
            firstTime = false;
            if (PigMapReduce.sJobConfInternal.get() != null) {
                String bagType = PigMapReduce.sJobConfInternal.get().get("pig.cachedbag.type");
                if (bagType != null && bagType.equalsIgnoreCase("default")) {
                    useDefaultBag = true;
                }
            }
        }
        
        if(distinct) {
            // only set the key which has the whole
            // tuple 
            res = mTupleFactory.newTuple(1);
            res.set(0, key);
        } else {
            //Create numInputs bags
            DataBag[] dbs = null;
            dbs = new DataBag[numInputs];
                 
            if (isAccumulative()) {
                // create bag wrapper to pull tuples in many batches
                // all bags have reference to the sample tuples buffer
                // which contains tuples from one batch
                POPackageTupleBuffer buffer = new POPackageTupleBuffer();
                for (int i = 0; i < numInputs; i++) {
                    dbs[i] = new AccumulativeBag(buffer, i);
                }
                
            } else {
                // create bag to pull all tuples out of iterator
                for (int i = 0; i < numInputs; i++) {
                    dbs[i] = useDefaultBag ? BagFactory.getInstance().newDefaultBag()
                    // In a very rare case if there is a POStream after this 
                    // POPackage in the pipeline and is also blocking the pipeline;
                    // constructor argument should be 2 * numInputs. But for one obscure
                    // case we don't want to pay the penalty all the time.                
                            : new InternalCachedBag(numInputs);                    
                }                               
                //For each indexed tup in the inp, sort them
                //into their corresponding bags based
                //on the index
                while (tupIter.hasNext()) {
                    NullableTuple ntup = tupIter.next();
                    int index = ntup.getIndex();
                    Tuple copy = getValueTuple(ntup, index);  
                    
                    if (numInputs == 1) {
                        
                        // this is for multi-query merge where 
                        // the numInputs is always 1, but the index
                        // (the position of the inner plan in the 
                        // enclosed operator) may not be 1.
                        dbs[0].add(copy);
                    } else {
                        dbs[index].add(copy);
                    }
                    if(reporter!=null) reporter.progress();
                }
            }
                      
            //Construct the output tuple by appending
            //the key and all the above constructed bags
            //and return it.
            res = mTupleFactory.newTuple(numInputs+1);
            res.set(0,key);
            int i=-1;
            for (DataBag bag : dbs) {
                i++;
                if(inner[i] && !isAccumulative()){
                    if(bag.size()==0){
                        detachInput();
                        Result r = new Result();
                        r.returnStatus = POStatus.STATUS_NULL;
                        return r;
                    }
                } 
                
                res.set(i+1,bag);
            }
        }
        Result r = new Result();
        r.returnStatus = POStatus.STATUS_OK;
        if (!isAccumulative())
            r.result = illustratorMarkup(null, res, 0);
        else
            r.result = res;
        detachInput();
        return r;
    }

    protected Tuple getValueTuple(NullableTuple ntup, int index) throws ExecException {
     // Need to make a copy of the value, as hadoop uses the same ntup
        // to represent each value.
        Tuple val = (Tuple)ntup.getValueAsPigType();
        
        Tuple copy = null;
        // The "value (val)" that we just got may not
        // be the complete "value". It may have some portions
        // in the "key" (look in POLocalRearrange for more comments)
        // If this is the case we need to stitch
        // the "value" together.        
        Pair<Boolean, Map<Integer, Integer>> lrKeyInfo = 
            keyInfo.get(index);           
        boolean isProjectStar = lrKeyInfo.first;
        Map<Integer, Integer> keyLookup = lrKeyInfo.second;
        int keyLookupSize = keyLookup.size();

        if( keyLookupSize > 0) {
        
            // we have some fields of the "value" in the
            // "key".
            int finalValueSize = keyLookupSize + val.size();
            copy = mTupleFactory.newTuple(finalValueSize);
            int valIndex = 0; // an index for accessing elements from 
                              // the value (val) that we have currently
            for(int i = 0; i < finalValueSize; i++) {
                Integer keyIndex = keyLookup.get(i);
                if(keyIndex == null) {
                    // the field for this index is not in the
                    // key - so just take it from the "value"
                    // we were handed
                    copy.set(i, val.get(valIndex));
                    valIndex++;
                } else {
                    // the field for this index is in the key
                    if(isKeyTuple && isKeyCompound) {
                        // the key is a tuple, extract the
                        // field out of the tuple
                        copy.set(i, keyAsTuple.get(keyIndex));
                    } else {
                        copy.set(i, key);
                    }
                }
            }
            copy = illustratorMarkup2(val, copy);
        } else if (isProjectStar) {
            
            // the whole "value" is present in the "key"
            copy = mTupleFactory.newTuple(keyAsTuple.getAll());
            copy = illustratorMarkup2(keyAsTuple, copy);
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
     * Make a deep copy of this operator.  
     * @throws CloneNotSupportedException
     */
    @Override
    public POPackage clone() throws CloneNotSupportedException {
        POPackage clone = (POPackage)super.clone();
        clone.mKey = new OperatorKey(mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope));
        clone.requestedParallelism = requestedParallelism;
        clone.resultType = resultType;
        clone.keyType = keyType;
        clone.numInputs = numInputs;
        if (inner!=null)
        {
            clone.inner = new boolean[inner.length];
            for (int i = 0; i < inner.length; i++) {
                clone.inner[i] = inner[i];
            }
        }
        else
            clone.inner = null;
        return clone;
    }

    /**
     * @param keyInfo the keyInfo to set
     */
    public void setKeyInfo(Map<Integer, Pair<Boolean, Map<Integer, Integer>>> keyInfo) {
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
    
    class POPackageTupleBuffer implements AccumulativeTupleBuffer {
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
            this.iter = tupIter;
            this.currKey = key;
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
                     Tuple copy = getValueTuple(ntup, index);		            
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
        
        public void clear() {
            for(int i=0; i<bags.length; i++) {
                bags[i].clear();
            }
            iter = null;
        }
        
        public Iterator<Tuple> getTuples(int index) {			
            return bags[index].iterator();
        }
        
        public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
            return POPackage.this.illustratorMarkup(in, out, eqClassIndex);
        }
       };
       
       private Tuple illustratorMarkup2(Object in, Object out) {
           if(illustrator != null) {
               ExampleTuple tOut = new ExampleTuple((Tuple) out);
               illustrator.getLineage().insert(tOut);
               tOut.synthetic = ((ExampleTuple) in).synthetic;
               illustrator.getLineage().union(tOut, (Tuple) in);
               return tOut;
           } else
               return (Tuple) out;
       }
       
       @Override
       public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
           if(illustrator != null) {
               ExampleTuple tOut = new ExampleTuple((Tuple) out);
               LineageTracer lineageTracer = illustrator.getLineage();
               lineageTracer.insert(tOut);
               Tuple tmp;
               boolean synthetic = false;
               if (illustrator.getEquivalenceClasses() == null) {
                   LinkedList<IdentityHashSet<Tuple>> equivalenceClasses = new LinkedList<IdentityHashSet<Tuple>>();
                   for (int i = 0; i < numInputs; ++i) {
                       IdentityHashSet<Tuple> equivalenceClass = new IdentityHashSet<Tuple>();
                       equivalenceClasses.add(equivalenceClass);
                   }
                   illustrator.setEquivalenceClasses(equivalenceClasses, this);
               }

               if (distinct) {
                   int count;
                   for (count = 0; tupIter.hasNext(); ++count) {
                       NullableTuple ntp = tupIter.next();
                       tmp = (Tuple) ntp.getValueAsPigType();
                       if (!tmp.equals(tOut))
                           lineageTracer.union(tOut, tmp);
                   }
                   if (count > 1) // only non-distinct tuples are inserted into the equivalence class
                       illustrator.getEquivalenceClasses().get(eqClassIndex).add(tOut);
                   illustrator.addData((Tuple) tOut);
                   return (Tuple) tOut;
               }
               boolean outInEqClass = true;
               try {
                   for (int i = 1; i < numInputs+1; i++)
                   {
                       DataBag dbs = (DataBag) ((Tuple) out).get(i);
                       Iterator<Tuple> iter = dbs.iterator();
                       if (dbs.size() <= 1 && outInEqClass) // all inputs have >= 2 records
                           outInEqClass = false;
                       while (iter.hasNext()) {
                           tmp = iter.next();
                           // any of synthetic data in bags causes the output tuple to be synthetic
                           if (!synthetic && ((ExampleTuple)tmp).synthetic)
                               synthetic = true;
                           lineageTracer.union(tOut, tmp);
                       }
                   }
               } catch (ExecException e) {
                 // TODO better exception handling
                 throw new RuntimeException("Illustrator exception :"+e.getMessage());
               }
               if (outInEqClass)
                   illustrator.getEquivalenceClasses().get(eqClassIndex).add(tOut);
               tOut.synthetic = synthetic;
               illustrator.addData((Tuple) tOut);
               return tOut;
           } else
               return (Tuple) out;
       }
}
