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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadCaster;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;

/**
 * A loader that samples the data.  This loader can subsume loader that
 * can handle starting in the middle of a record.  Loaders that can
 * handle this should implement the SamplableLoader interface.
 */
//XXX : FIXME - make this work with new load-store redesign
public class RandomSampleLoader extends SampleLoader {
 
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
    
    
    @Override
    public void setNumSamples(int n) {
    	// Setting it to 100 as default for order by
    	super.setNumSamples(100);
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#getInputFormat()
     */
    @Override
    public InputFormat getInputFormat() {
        // TODO Auto-generated method stub
        return null;
    }


    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#getLoadCaster()
     */
    @Override
    public LoadCaster getLoadCaster() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#prepareToRead(org.apache.hadoop.mapreduce.RecordReader, org.apache.hadoop.mapreduce.InputSplit)
     */
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        // TODO Auto-generated method stub
        
    }


    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#setLocation(java.lang.String, org.apache.hadoop.mapreduce.Job)
     */
    @Override
    public void setLocation(String location, Job job) throws IOException {
        // TODO Auto-generated method stub
        
    }


    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#relativeToAbsolutePath(java.lang.String, org.apache.hadoop.fs.Path)
     */
    @Override
    public String relativeToAbsolutePath(String location, Path curDir)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
 
}
