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
package org.apache.pig.impl.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;

/**
 * This is wrapper Loader which wraps a real LoadFunc underneath and allows
 * to read a file completely starting a given split (indicated by a split index 
 * which is used to look in the List<InputSplit> returned by the underlying
 * InputFormat's getSplits() method). So if the supplied split index is 0, this
 * loader will read the entire file. If it is non zero it will read the partial
 * file beginning from that split to the last split.
 * 
 * The call sequence to use this is:
 * 1) construct an object using the constructor
 * 2) Call getNext() in a loop till it returns null
 */
public class ReadToEndLoader implements LoadFunc {

    /**
     * the wrapped LoadFunc which will do the actual reading
     */
    private LoadFunc wrappedLoadFunc;
    
    /**
     * the Configuration object used to locate the input location - this will
     * be used to call {@link LoadFunc#setLocation(String, Configuration)} on 
     * the wrappedLoadFunc
     */
    private Configuration conf;
    
    /**
     * the input location string (typically input file/dir name )
     */
    private String inputLocation;
    
    /**
     * the index of the split (in {@link InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)})
     * to start reading from
     */
    private int startSplitIndex;

    /**
     * the index of the split the loader is currently reading from
     */
    private int curSplitIndex;
    
    /**
     * the input splits returned by underlying {@link InputFormat#getSplits(JobContext)}
     */
    private List<InputSplit> splits;
    
    /**
     * underlying RecordReader
     */
    private RecordReader reader;
    
    /**
     * underlying InputFormat
     */
    private InputFormat inputFormat;
    
    /**
     * @param wrappedLoadFunc
     * @param conf
     * @param inputLocation
     * @param splitIndex
     * @throws IOException 
     * @throws InterruptedException 
     */
    public ReadToEndLoader(LoadFunc wrappedLoadFunc, Configuration conf,
            String inputLocation, int splitIndex) throws IOException {
        this.wrappedLoadFunc = wrappedLoadFunc;
        // make a copy so that if the underlying InputFormat writes to the
        // conf, we don't affect the caller's copy
        this.conf = new Configuration(conf);
        this.inputLocation = inputLocation;
        this.startSplitIndex = splitIndex;
        this.curSplitIndex = startSplitIndex;
        
        // let's initialize the wrappedLoadFunc 
        Job job = new Job(this.conf);
        wrappedLoadFunc.setLocation(this.inputLocation, 
                job);
        // The above setLocation call could write to the conf within
        // the job - get a hold of the modified conf
        this.conf = job.getConfiguration();
        inputFormat = wrappedLoadFunc.getInputFormat();
        try {
            splits = inputFormat.getSplits(new JobContext(this.conf,
                    new JobID()));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
    
    private boolean initializeReader() throws IOException, 
    InterruptedException {
        if(curSplitIndex > splits.size() - 1) {
            // past the last split, we are done
            return false;
        }
        
        InputSplit curSplit = splits.get(curSplitIndex);
        TaskAttemptContext tAContext = new TaskAttemptContext(conf, 
                new TaskAttemptID());
        reader = inputFormat.createRecordReader(curSplit, tAContext);
        reader.initialize(curSplit, tAContext);
        // create a dummy pigsplit - other than the actual split, the other
        // params are really not needed here where we are just reading the
        // input completely
        PigSplit pigSplit = new PigSplit(curSplit, -1, 
                new ArrayList<OperatorKey>(), -1);
        wrappedLoadFunc.prepareToRead(reader, pigSplit);
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#getNext()
     */
    public Tuple getNext() throws IOException {
        try {
            Tuple t = null;
            if(reader == null) {
                // first call
                return getNextHelper();
            } else {
                // we already have a reader initialized
                t = wrappedLoadFunc.getNext();
                if(t != null) {
                    return t;
                }
                // if loadfunc returned null, we need to read next split
                // if there is one
                curSplitIndex++;
                return getNextHelper();
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
    
    private Tuple getNextHelper() throws IOException, InterruptedException {
        Tuple t = null;
        while(initializeReader()) {
            t = wrappedLoadFunc.getNext();
            if(t == null) {
                // try next split
                curSplitIndex++;
            } else {
                return t;
            }
        }
        // we processed all splits - we are done
        wrappedLoadFunc.doneReading();
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#doneReading()
     */
    @Override
    public void doneReading() {
        throw new RuntimeException("Internal Error: Unimplemented method called!");        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#getInputFormat()
     */
    @Override
    public InputFormat getInputFormat() {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#getLoadCaster()
     */
    @Override
    public LoadCaster getLoadCaster() {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#prepareToRead(org.apache.hadoop.mapreduce.RecordReader, org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit)
     */
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        throw new RuntimeException("Internal Error: Unimplemented method called!");        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#setLocation(java.lang.String, org.apache.hadoop.mapreduce.Job)
     */
    @Override
    public void setLocation(String location, Job job) throws IOException {
        throw new RuntimeException("Internal Error: Unimplemented method called!");        
    }
   
}
