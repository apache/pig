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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.InternalCachedBag;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;
/**
 * The package operator that packages the globally rearranged tuples into
 * output format after the combiner stage.  It differs from POPackage in that
 * it does not use the index in the NullableTuple to find the bag to put a
 * tuple in.  Instead, the inputs are put in a bag corresponding to their 
 * offset in the tuple.
 */
public class POCombinerPackage extends POPackage {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private static BagFactory mBagFactory = BagFactory.getInstance();
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    private boolean[] mBags; // For each field, indicates whether or not it
                             // needs to be put in a bag.
    
    private Map<Integer, Integer> keyLookup;
    
    private int numBags;

    /**
     * A new POPostCombinePackage will be constructed as a near clone of the
     * provided POPackage.
     * @param pkg POPackage to clone.
     * @param bags for each field, indicates whether it should be a bag (true)
     * or a simple field (false).
     */
    public POCombinerPackage(POPackage pkg, boolean[] bags) {
        super(new OperatorKey(pkg.getOperatorKey().scope,
            NodeIdGenerator.getGenerator().getNextNodeId(pkg.getOperatorKey().scope)),
            pkg.getRequestedParallelism(), pkg.getInputs());
        resultType = pkg.getResultType();
        keyType = pkg.keyType;
        numInputs = 1;
        inner = new boolean[1];
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
    public String name() {
        return "POCombinerPackage" + "[" + DataType.findTypeName(resultType) + "]" + "{" + DataType.findTypeName(keyType) + "}" +" - " + mKey.toString();
    }
    
    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitCombinerPackage(this);
    }
    
    /**
     * @param keyInfo the keyInfo to set
     */
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
    	String bagType = null;
        if (PigMapReduce.sJobConfInternal.get() != null) {
   			bagType = PigMapReduce.sJobConfInternal.get().get("pig.cachedbag.type");       			
   	    }
                		          	           		
    	if (bagType != null && bagType.equalsIgnoreCase("default")) {
    		return new NonSpillableDataBag();
    	}
    	return new InternalCachedBag(numBags);  	
    }
    
    @Override
    public Result getNext(Tuple t) throws ExecException {
        int keyField = -1;
        //Create numInputs bags
        Object[] fields = new Object[mBags.length];
        for (int i = 0; i < mBags.length; i++) {
            if (mBags[i]) fields[i] = createDataBag(numBags);            
        }
        
        // For each indexed tup in the inp, split them up and place their
        // fields into the proper bags.  If the given field isn't a bag, just
        // set the value as is.
        while (tupIter.hasNext()) {
            NullableTuple ntup = tupIter.next();
            Tuple tup = (Tuple)ntup.getValueAsPigType();
            
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
        
        // The successor of the POCombinerPackage as of 
        // now SHOULD be a POForeach which has been adjusted
        // to look for its inputs by projecting from the corresponding
		// positions in the POCombinerPackage output.
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

}
