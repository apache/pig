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
import java.util.Random;

import org.apache.pig.data.Tuple;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;

/**
 * A loader that samples the data.  
 * It randomly samples tuples from input. The number of tuples to be sampled
 * has to be set before the first call to getNext().
 *  see documentation of getNext() call.
 */
public class RandomSampleLoader extends SampleLoader {
 
    //array to store the sample tuples
    Tuple [] samples = null;
    //index into samples array to the next sample to be returned 
    protected int nextSampleIdx= 0;
    
    /**
     * Construct with a class of loader to use.
     * @param funcSpec func spec of the loader to use.
     * @param ns Number of samples per map to collect. 
     * Arguments are passed as strings instead of actual types (FuncSpec, int) 
     * because FuncSpec only supports string arguments to
     * UDF constructors.
     */
    public RandomSampleLoader(String funcSpec, String ns) {
    	// instantiate the loader
        super(funcSpec);
        // set the number of samples
        super.setNumSamples(Integer.valueOf(ns));
    }

    /**
     * Allocate a buffer for numSamples elements, populate it with the 
     * first numSamples tuples, and continue scanning rest of the input.
     * For every ith next() call, we generate a random number r s.t. 0<=r<i,
     * and if r<numSamples we insert the new tuple into our buffer at position r.
     * This gives us a random sample of the tuples in the partition.
     */
    @Override
    public Tuple getNext() throws IOException {

        if(samples != null){
            return getSample();
        }
        //else collect samples
        samples = new Tuple[numSamples];
        
        // populate the samples array with first numSamples tuples
        Tuple t = null;
        for(int i=0; i<numSamples; i++){
            t = loader.getNext();
            if(t == null)
                break;
            samples[i] = t;
        }
        
        // rowNum that starts from 1
        int rowNum = numSamples+1;
        Random randGen = new Random();

        if(t != null){ // did not exhaust all tuples
            while(true){
                // collect samples until input is exhausted
                int rand = randGen.nextInt(rowNum);
                if(rand < numSamples){
                    // pick this as sample
                    Tuple sampleTuple = loader.getNext();
                    if(sampleTuple == null)
                        break;
                    samples[rand] = sampleTuple;
                }else {
                    //skip tuple
                    if(!skipNext())
                        break;
                }
                rowNum++;
            }
        }        
        
        return getSample();
    } 
    
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        super.prepareToRead(reader, split);
        samples = null;
        nextSampleIdx = 0;
    }
    
    private Tuple getSample() {
        if(nextSampleIdx < samples.length){
            return samples[nextSampleIdx++];
        }
        else{
            return null;
        }
    }

}
