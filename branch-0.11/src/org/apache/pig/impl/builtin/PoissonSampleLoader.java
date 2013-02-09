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
import java.util.ArrayList;
import java.util.Properties;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.util.Pair;

/**
 * See "Skewed Join sampler" in http://wiki.apache.org/pig/PigSampler
 */
public class PoissonSampleLoader extends SampleLoader {
    
    // marker string to mark the last sample row, which has total number or rows
    // seen by this map instance
    // this string will be in the 2nd last column of the last sample row
    // it is used by GetMemNumRows
    public static final String NUMROWS_TUPLE_MARKER = 
        "\u4956\u3838_pig_inTeRnal-spEcial_roW_num_tuple3kt579CFLehkblah";
    
    //num of rows sampled so far
    private int numRowsSampled = 0;
    
    //average size of tuple in memory, for tuples sampled
    private long avgTupleMemSz = 0;
    
    //current row number 
    private long rowNum = 0;
    
    // number of tuples to skip after each sample
    long skipInterval = -1;

    // bytes in input to skip after every sample. 
    // divide this by avgTupleMemSize to get skipInterval 
    private long memToSkipPerSample = 0;
    
    // has the special row with row number information been returned
    private boolean numRowSplTupleReturned = false;
    
    /// For a given mean and a confidence, a sample rate is obtained from a poisson cdf
    private static final String SAMPLE_RATE = "pig.sksampler.samplerate";
    
    // 17 is not a magic number. It can be obtained by using a poisson cumulative distribution function with the mean
    // set to 10 (emperically, minimum number of samples) and the confidence set to 95%
    private static final int DEFAULT_SAMPLE_RATE = 17;
        
    private int sampleRate = DEFAULT_SAMPLE_RATE;
    
    /// % of memory available for the input data. This is currenty equal to the memory available
    /// for the skewed join
    private static final String PERC_MEM_AVAIL = "pig.skewedjoin.reduce.memusage";

    private double heapPerc = PartitionSkewedKeys.DEFAULT_PERCENT_MEMUSAGE;
    
    // new Sample tuple
    private Tuple newSample = null;
        
//  private final Log log = LogFactory.getLog(getClass());
    
    public PoissonSampleLoader(String funcSpec, String ns) {
        super(funcSpec);
        super.setNumSamples(Integer.valueOf(ns)); // will be overridden
    }

    @Override
    public Tuple getNext() throws IOException {
        if(numRowSplTupleReturned){
            // row num special row has been returned after all inputs 
            // were read, nothing more to read 
            return null;
        }


        if(skipInterval == -1){
            //select first tuple as sample and calculate
            // number of tuples to be skipped 
            Tuple t = loader.getNext();
            if(t == null)
                return createNumRowTuple(null);
            long availRedMem = (long) (Runtime.getRuntime().maxMemory() * heapPerc);
            memToSkipPerSample = availRedMem/sampleRate;
            updateSkipInterval(t);

            rowNum++;
            newSample = t;
        }

        // skip tuples
        for(long numSkipped  = 0; numSkipped < skipInterval; numSkipped++){
            if(!skipNext()){
                return createNumRowTuple(newSample);
            }
            rowNum++;
        }

        // skipped enough, get new sample
        Tuple t = loader.getNext();
        if(t == null)
            return createNumRowTuple(newSample);
        updateSkipInterval(t);
        rowNum++;
        Tuple currentSample = newSample;
        newSample = t;
        return currentSample;
    }

    /**
     * Update the average tuple size base on newly sampled tuple t
     * and recalculate skipInterval
     * @param t - tuple
     */
    private void updateSkipInterval(Tuple t) {
        avgTupleMemSz = 
            ((avgTupleMemSz*numRowsSampled) + t.getMemorySize())/(numRowsSampled + 1);
        skipInterval = memToSkipPerSample/avgTupleMemSz;
        
            // skipping fewer number of rows the first few times, to reduce 
            // the probability of first tuples size (if much smaller than rest) 
        // resulting in 
            // very few samples being sampled. Sampling a little extra is OK
        if(numRowsSampled < 5)
            skipInterval = skipInterval/(10-numRowsSampled);
        ++numRowsSampled;

    }

    /**
     * @param sample - sample tuple
     * @return - Tuple appended with special marker string column, num-rows column
     * @throws ExecException
     */
    private Tuple createNumRowTuple(Tuple sample) throws ExecException {
        int sz = (sample == null) ? 0 : sample.size();
        TupleFactory factory = TupleFactory.getInstance();       
        Tuple t = factory.newTuple(sz + 2);
 
        if (sample != null) {
            for(int i=0; i<sample.size(); i++){
                t.set(i, sample.get(i));
            }
        }
        
        t.set(sz, NUMROWS_TUPLE_MARKER);
        t.set(sz + 1, rowNum);
        numRowSplTupleReturned = true;
        return t;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        super.prepareToRead(reader, split);
        numRowsSampled = 0;
        avgTupleMemSz = 0;
        rowNum = 0;
        skipInterval = -1;
        memToSkipPerSample = 0;
        numRowSplTupleReturned = false;
        newSample = null;

        Configuration conf = split.getConf();
        sampleRate = conf.getInt(SAMPLE_RATE, DEFAULT_SAMPLE_RATE);
        heapPerc = conf.getFloat(PERC_MEM_AVAIL, PartitionSkewedKeys.DEFAULT_PERCENT_MEMUSAGE);
    }

}
