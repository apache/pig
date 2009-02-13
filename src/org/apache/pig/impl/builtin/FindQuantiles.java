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
import java.util.Comparator;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;


public class FindQuantiles extends EvalFunc<DataBag>{
    BagFactory mBagFactory = BagFactory.getInstance();
    boolean[] mAsc;
    enum State { ALL_ASC, ALL_DESC, MIXED };
    State mState;
    
    private class SortComparator implements Comparator<Tuple> {
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

    public FindQuantiles() {
        mState = State.ALL_ASC;
    }

    public FindQuantiles(String[] args) {
        mAsc = new boolean[args.length];
        boolean sawAsc = false;
        boolean sawDesc = false;
        for (int i = 0; i < args.length; i++) {
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
    public DataBag exec(Tuple in) throws IOException {
        if(in==null || in.size()==0)
            return null;
        Integer numQuantiles = null;
        DataBag samples = null;
        
        try{
            Tuple input = (Tuple) in.get(0);
            numQuantiles = (Integer)input.get(0);
            samples = (DataBag)input.get(1);
        }catch(ExecException e){
            throw e;
        }
        // TODO If user provided a comparator we should be using that.
        DataBag output = mBagFactory.newSortedBag(mComparator);
        
        long numSamples = samples.size();
        
        long toSkip = numSamples / numQuantiles;
        
        long i=0, nextQuantile = 0;
        Iterator<Tuple> iter = samples.iterator();
        while (iter.hasNext()){
            Tuple t = iter.next();
            if (i==nextQuantile){
                output.add(t);
                nextQuantile+=toSkip+1;
            }
            i++;
            if (i % 1000 == 0) progress();
        }
        return output;
    }
}
