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
package org.apache.pig.test;


import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.DiscreteProbabilitySampleGenerator;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.InternalMap;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.builtin.FindQuantiles;
import org.junit.Test;

public class TestFindQuantiles {
    
    private static TupleFactory tFact = TupleFactory.getInstance();
    private static final float epsilon = 0.00001f;
    
    @Test
    public void testFindQuantiles() throws Exception {
       final int numSamples = 97778;
       final int numReducers = 1009;
       float sum = getProbVecSum(numSamples, numReducers);
       System.out.println("sum: " + sum);
       assertTrue(sum > (1+epsilon));
    }
    
    @Test
    public void testFindQuantiles2() throws Exception {
       final int numSamples = 30000;
       final int numReducers = 3000;
       float sum = getProbVecSum(numSamples, numReducers);
       System.out.println("sum: " + sum);
       assertTrue(sum < (1-epsilon));
    }
    
    private float[] getProbVec(Tuple values) throws Exception {
        float[] probVec = new float[values.size()];        
        for(int i = 0; i < values.size(); i++) {
            probVec[i] = (Float)values.get(i);
        }
        return probVec;
    }
    
    private float getProbVecSum(int numSamples, int numReduceres) throws Exception {
        Tuple in = tFact.newTuple(2);
        DataBag samples = BagFactory.getInstance().newDefaultBag(); 
        for (int i=0; i<numSamples; i++) {
            Tuple t = tFact.newTuple(1);
            t.set(0, new Integer(23));
            samples.add(t);
        }
        in.set(0, new Integer(numReduceres));
        in.set(1, samples);
        
        FindQuantiles fq = new FindQuantiles();
        
        Map<String, Object> res = fq.exec(in);
        
        InternalMap weightedPartsData = (InternalMap) res.get(FindQuantiles.WEIGHTED_PARTS);
        Iterator<Object> it = weightedPartsData.values().iterator();
        float[] probVec = getProbVec((Tuple)it.next());
        new DiscreteProbabilitySampleGenerator(probVec);
        float sum = 0.0f;
        for (float f : probVec) {
            sum += f;
        }
        return sum;
    }
    
}
