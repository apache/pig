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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.DiscreteProbabilitySampleGenerator;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.InternalMap;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.builtin.FindQuantiles;
import org.junit.Test;

public class TestFindQuantiles {
    
    private static TupleFactory tFact = TupleFactory.getInstance();
    private static final float epsilon = 0.0001f;
    
    @Test
    public void testFindQuantiles() throws Exception {
       final int numSamples = 97778;
       final int numReducers = 1009;
       float sum = getProbVecSum(numSamples, numReducers);
       System.out.println("sum: " + sum);
       assertTrue(sum > (1-epsilon) && sum < (1+epsilon));
    }
    
    @Test
    public void testFindQuantiles2() throws Exception {
       final int numSamples = 30000;
       final int numReducers = 3000;
       float sum = getProbVecSum(numSamples, numReducers);
       System.out.println("sum: " + sum);
       assertTrue(sum > (1-epsilon) && sum < (1+epsilon));
    }

    @Test
    public void testFindQuantilesRemainder() throws Exception {
       final int numSamples = 1900;
       final int numReducers = 300;
       DataBag samples = generateRandomSortedSamples(numSamples, 365);
       Map<String, Object> findQuantilesResult = getFindQuantilesResult(samples, numReducers);
       DataBag quantilesBag = (DataBag)findQuantilesResult.get(FindQuantiles.QUANTILES_LIST);
       Iterator<Tuple> iter = quantilesBag.iterator();
       Tuple lastQuantile = null;
       while (iter.hasNext()) {
           lastQuantile = iter.next();
       }
       int lastQuantileNum = (Integer)lastQuantile.get(0);
       int count = 0;
       iter = samples.iterator();
       while (iter.hasNext()) {
           Tuple t = iter.next();
           int num = (Integer)t.get(0);
           if (num >= lastQuantileNum) {
               count++;
           }
       }
       assertTrue((double)count/numSamples <= 1.0/365 + 0.001);
    }

    private float[] getProbVec(Tuple values) throws Exception {
        float[] probVec = new float[values.size()];        
        for(int i = 0; i < values.size(); i++) {
            probVec[i] = (Float)values.get(i);
        }
        return probVec;
    }

    private DataBag generateRandomSortedSamples(int numSamples, int max) throws Exception {
        Random rand = new Random(1000);
        List<Tuple> samples = new ArrayList<Tuple>(); 
        for (int i=0; i<numSamples; i++) {
            Tuple t = tFact.newTuple(1);
            t.set(0, rand.nextInt(max));
            samples.add(t);
        }
        Collections.sort(samples);
        return new NonSpillableDataBag(samples);
    }

    private DataBag generateUniqueSamples(int numSamples) throws Exception {
        DataBag samples = BagFactory.getInstance().newDefaultBag(); 
        for (int i=0; i<numSamples; i++) {
            Tuple t = tFact.newTuple(1);
            t.set(0, new Integer(23));
            samples.add(t);
        }
        return samples;
    }

    private Map<String, Object> getFindQuantilesResult(DataBag samples,
            int numReduceres) throws Exception {
        Tuple in = tFact.newTuple(2);

        in.set(0, new Integer(numReduceres));
        in.set(1, samples);
        
        FindQuantiles fq = new FindQuantiles();
        
        Map<String, Object> res = fq.exec(in);
        return res;
    }

    private float getProbVecSum(int numSamples, int numReduceres) throws Exception {
        DataBag samples = generateUniqueSamples(numSamples);
        Map<String, Object> res = getFindQuantilesResult(samples, numReduceres);

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
