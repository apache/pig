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
package org.apache.pig.impl.builtin;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.ComparisonFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.CountingMap;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.InternalMap;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;


public class FindQuantiles extends EvalFunc<Map<String, Object>>{
    // keys for the weightedparts Map
    public static final String QUANTILES_LIST = "quantiles.list";
    public static final String WEIGHTED_PARTS = "weighted.parts";

    BagFactory mBagFactory = BagFactory.getInstance();
    TupleFactory mTupleFactory = TupleFactory.getInstance();

    boolean[] mAsc;
    enum State { ALL_ASC, ALL_DESC, MIXED };
    State mState;
    
    protected Integer numQuantiles = null;
    protected DataBag samples = null;
    
    private class SortComparator implements Comparator<Tuple> {
        @Override
        @SuppressWarnings("unchecked")
        public int compare(Tuple t1, Tuple t2) {
            switch (mState) {
            case ALL_ASC:
                return t1.compareTo(t2);

            case ALL_DESC:
                return t2.compareTo(t1);

            case MIXED:
                // Have to break the tuple down and compare it field to field.
                int sz1 = t1.size();
                int sz2 = t2.size();
                if (sz2 < sz1) {
                    return 1;
                } else if (sz2 > sz1) {
                    return -1;
                } else {
                    for (int i = 0; i < sz1; i++) {
                        try {
                            int c = DataType.compare(t1.get(i), t2.get(i));
                            if (c != 0) {
                                if (!mAsc[i]) c *= -1;
                                return c;
                            }
                        } catch (ExecException e) {
                            throw new RuntimeException("Unable to compare tuples", e);
                        }
                    }
                    return 0;
                }
            }
            return -1; // keep the compiler happy
        }
    }

    private Comparator<Tuple> mComparator = new SortComparator();
    private FuncSpec mUserComparisonFuncSpec;
    private ComparisonFunc mUserComparisonFunc;
    
    
    @SuppressWarnings("unchecked")
    private void instantiateFunc() {
        if(mUserComparisonFunc != null) {
            this.mUserComparisonFunc = (ComparisonFunc) PigContext.instantiateFuncFromSpec(this.mUserComparisonFuncSpec);
            this.mUserComparisonFunc.setReporter(reporter);
            this.mComparator = mUserComparisonFunc;
        }
    }
    
    // We need to instantiate any user defined comparison function 
    // on the backend when the FindQuantiles udf is deserialized
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException{
        is.defaultReadObject();
        instantiateFunc();
    }
    

    public FindQuantiles() {
        mState = State.ALL_ASC;
    }

    public FindQuantiles(String[] args) {
        int startIndex = 0;
        int ascFlagsLength = args.length;
        // the first argument may be the information
        // about user defined comparison function if one
        // was specified
        if(args[0].startsWith(MRCompiler.USER_COMPARATOR_MARKER)) {
            mUserComparisonFuncSpec = new FuncSpec(
                    args[0].substring(MRCompiler.USER_COMPARATOR_MARKER.length()));
            // skip the first argument now that we used it
            startIndex++;
            ascFlagsLength--;
        }
        
        mAsc = new boolean[ascFlagsLength];
        boolean sawAsc = false;
        boolean sawDesc = false;
        for (int i = startIndex; i < ascFlagsLength; i++) {
            mAsc[i] = Boolean.parseBoolean(args[i]);
            if (mAsc[i]) sawAsc = true;
            else sawDesc = true;
        }
        if (sawAsc && sawDesc) mState = State.MIXED;
        else if (sawDesc) mState = State.ALL_DESC;
        else mState = State.ALL_ASC; // In cast they gave us no args this
                                     // defaults to all ascending.
    }

    /**
     * first field in the input tuple is the number of quantiles to generate
     * second field is the *sorted* bag of samples
     */
    
    @Override
    public Map<String, Object> exec(Tuple in) throws IOException {
        Map<String, Object> output = new HashMap<String, Object>();
        if(in==null || in.size()==0)
            return null;
        
        ArrayList<Tuple> quantilesList = new ArrayList<Tuple>();
        InternalMap weightedParts = new InternalMap();
        // the sample file has a tuple as under:
        // (numQuantiles, bag of samples) 
        // numQuantiles here is the reduce parallelism
        try{
            if (numQuantiles == null) {
                numQuantiles = (Integer)in.get(0);
            }
            if (samples == null) {
                samples = (DataBag)in.get(1);
            }
            long numSamples = samples.size();
            long toSkip = numSamples / numQuantiles;
            if(toSkip == 0) {
                // numSamples is < numQuantiles;
                // set numQuantiles to numSamples
                numQuantiles = (int)numSamples;
                toSkip = 1;
            }
            
            long ind=0, j=-1, nextQuantile = toSkip-1;
            for (Tuple it : samples) {
                if (ind==nextQuantile){
                    ++j;
                    quantilesList.add(it);
                    nextQuantile+=toSkip;
                    if(j==numQuantiles-1)
                        break;
                }
                ind++;
                if (ind % 1000 == 0) progress();
            }
            long i=-1;
            Map<Tuple,CountingMap<Integer>> contribs = new HashMap<Tuple, CountingMap<Integer>>();
            for (Tuple it : samples){
                ++i;
                if (i % 1000 == 0) progress();
                int partInd = (int)(i/toSkip); // which partition
                if(partInd==numQuantiles) break;
                // the quantiles array has the element from the sample which is the
                // last element for a given partition. For example: if numQuantiles 
                // is 5 and number of samples is 100, then toSkip = 20 
                // quantiles[0] = sample[19] // the 20th element
                // quantiles[1] = sample[39] // the 40th element
                // and so on. For any element in the sample between 0 and 19, partInd
                // will be 0. We want to check if a sample element which is
                // present between 0 and 19 is also the 19th (quantiles[0] element).
                // This would mean that element might spread over the 0th and 1st 
                // partition. We are looking for contributions to a partition
                // from such elements. 
                
                // First We only check for sample elements in partitions other than the last one
                // < numQuantiles -1 (partInd is 0 indexed). 
                if(partInd<numQuantiles-1 && areEqual(it,quantilesList.get(partInd))){
                    if(!contribs.containsKey(it)){
                        CountingMap<Integer> cm = new CountingMap<Integer>();
                        cm.put(partInd, 1);
                        contribs.put(it, cm);
                    }
                    else
                        contribs.get(it).put(partInd, 1);
                }
                else{ 
                    // we are either in the last partition (last quantile)
                    // OR the sample element we are currently processing is not
                    // the same as the element in the quantile array for this partition
                    // if we haven't seen this sample item earlier, this is not an
                    // element which crosses partitions - so ignore
                    if(!contribs.containsKey(it))
                        continue;
                    else
                        // we have seen this sample before (in a previous partInd), 
                        // add to the contribution associated with this sample - if we had 
                        // not seen this sample in a previous partInd, then we would have not
                        // had this in the contribs map! (because of the if above).This 
                        // "key" (represented by the sample item) can either go to the 
                        // previous partInd or this partInd in the final sort reduce stage. 
                        // That is where the amount of contribution to each partInd will
                        // matter and influence the choice.
                        contribs.get(it).put(partInd, 1);
                }
            }
            int k = 0;
            for(Entry<Tuple, CountingMap<Integer>> ent : contribs.entrySet()){
                if (k % 1000 == 0) progress();
                Tuple key = ent.getKey(); // sample item which repeats
                
                // this map will have the contributions of the sample item to the different partitions
                CountingMap<Integer> value = ent.getValue(); 
                
                long total = value.getTotalCount();
                Tuple probVec =  mTupleFactory.newTuple(numQuantiles.intValue());
                // initialize all contribution fractions for different
                // partitions to 0.0
                for (int l = 0; l < numQuantiles; l++) {
                    probVec.set(l, new Float(0.0));
                }
                // for each partition that this sample item is present in,
                // compute the fraction of the total occurrences for that
                // partition - this will be the probability with which we
                // will pick this partition in the final sort reduce job
                // for this sample item
                for (Entry<Integer,Integer> valEnt : value.entrySet()) {
                    probVec.set(valEnt.getKey(), (float)valEnt.getValue()/total);
                }
                weightedParts.put(key, probVec);
            }
            output.put(QUANTILES_LIST, new NonSpillableDataBag(quantilesList));
            output.put(WEIGHTED_PARTS, weightedParts);
            return output;
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private boolean areEqual(Tuple it, Tuple tuple) {
        return mComparator.compare(it, tuple)==0;
    }
}
