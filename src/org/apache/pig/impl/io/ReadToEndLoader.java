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
import org.apache.pig.Expression;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
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
public class ReadToEndLoader extends LoadFunc implements LoadMetadata {

    /**
     * the wrapped LoadFunc which will do the actual reading
     */
    private LoadFunc wrappedLoadFunc;
    
    /**
     * the Configuration object used to locate the input location - this will
     * be used to call {@link LoadFunc#setLocation(String, Job)} on
     * the wrappedLoadFunc
     */
    private Configuration conf;
    
    /**
     * the input location string (typically input file/dir name )
     */
    private String inputLocation;
      
    /**
     * If the splits to be read are not in increasing sequence of integers
     * this array can be used
     */
    private int[] toReadSplits = null;
    
    /**
     * index into toReadSplits
     */
    private int toReadSplitsIdx = 0;
    
    /**
     * the index of the split the loader is currently reading from
     */
    private int curSplitIndex;
    
    /**
     * the input splits returned by underlying {@link InputFormat#getSplits(JobContext)}
     */
    private List<InputSplit> inpSplits = null;
    
    /**
     * underlying RecordReader
     */
    private RecordReader reader = null;
    
    /**
     * underlying InputFormat
     */
    private InputFormat inputFormat = null;
    
    private PigContext pigContext;
    
    private String udfContextSignature = null;

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
        this.inputLocation = inputLocation;
        this.conf = conf;
        this.curSplitIndex = splitIndex;
        init();
    }
    
    public ReadToEndLoader(LoadFunc wrappedLoadFunc, Configuration conf,
            String inputLocation, int splitIndex, PigContext pigContext) throws IOException {
        this.wrappedLoadFunc = wrappedLoadFunc;
        this.inputLocation = inputLocation;
        this.conf = conf;
        this.curSplitIndex = splitIndex;
        this.pigContext = pigContext;
        init();
    }
    
    public ReadToEndLoader(LoadFunc wrappedLoadFunc, Configuration conf,
            String inputLocation, int splitIndex, String signature) throws IOException {
        this.udfContextSignature = signature;
        this.wrappedLoadFunc = wrappedLoadFunc;
        this.inputLocation = inputLocation;
        this.conf = conf;
        this.curSplitIndex = splitIndex;
        init();
    }

    /**
     * This constructor takes an array of split indexes (toReadSplitIdxs) of the 
     * splits to be read.
     * @param wrappedLoadFunc
     * @param conf
     * @param inputLocation
     * @param toReadSplitIdxs
     * @throws IOException 
     * @throws InterruptedException 
     */
    public ReadToEndLoader(LoadFunc wrappedLoadFunc, Configuration conf,
            String inputLocation, int[] toReadSplitIdxs) throws IOException {
        this.wrappedLoadFunc = wrappedLoadFunc;
        this.inputLocation = inputLocation;
        this.toReadSplits = toReadSplitIdxs;
        this.conf = conf;
        this.curSplitIndex =
            toReadSplitIdxs.length > 0 ? toReadSplitIdxs[0] : Integer.MAX_VALUE;
        init();
    }
    
    @SuppressWarnings("unchecked")
    private void init() throws IOException {
        if (conf != null && pigContext != null) {
            SchemaTupleBackend.initialize(conf, pigContext, true);
        }

        // make a copy so that if the underlying InputFormat writes to the
        // conf, we don't affect the caller's copy
        conf = new Configuration(conf);

        // let's initialize the wrappedLoadFunc 
        Job job = new Job(conf);
        wrappedLoadFunc.setUDFContextSignature(this.udfContextSignature);
        wrappedLoadFunc.setLocation(inputLocation, 
                job);
        // The above setLocation call could write to the conf within
        // the job - get a hold of the modified conf
        conf = job.getConfiguration();
        inputFormat = wrappedLoadFunc.getInputFormat();
        try {
            inpSplits = inputFormat.getSplits(HadoopShims.createJobContext(conf,
                    new JobID()));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }        
    }

    private boolean initializeReader() throws IOException, 
    InterruptedException {
        if(curSplitIndex > inpSplits.size() - 1) {
            // past the last split, we are done
            return false;
        }
        if(reader != null){
            reader.close();
        }
        InputSplit curSplit = inpSplits.get(curSplitIndex);
        TaskAttemptContext tAContext = HadoopShims.createTaskAttemptContext(conf, 
                new TaskAttemptID());
        reader = inputFormat.createRecordReader(curSplit, tAContext);
        reader.initialize(curSplit, tAContext);
        // create a dummy pigsplit - other than the actual split, the other
        // params are really not needed here where we are just reading the
        // input completely
        PigSplit pigSplit = new PigSplit(new InputSplit[] {curSplit}, -1, 
                new ArrayList<OperatorKey>(), -1);
        wrappedLoadFunc.prepareToRead(reader, pigSplit);
        return true;
    }

    @Override
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
                updateCurSplitIndex();
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
                updateCurSplitIndex();
            } else {
                return t;
            }
        }
        return null;
    }
    
    
    /**
     * Updates curSplitIndex , just increment if splitIndexes is null,
     * else get next split in splitIndexes
     */
    private void updateCurSplitIndex() {
        if(toReadSplits == null){
            ++curSplitIndex;
        }else{
            ++toReadSplitsIdx;
            if(toReadSplitsIdx >= toReadSplits.length){
                // finished all the splits in splitIndexes array
                curSplitIndex = Integer.MAX_VALUE;
            }else{
                curSplitIndex = toReadSplits[toReadSplitsIdx];
            }
        }
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public LoadCaster getLoadCaster() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        wrappedLoadFunc.setLocation(location, job);
    }

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
        if (wrappedLoadFunc instanceof LoadMetadata) {
            return ((LoadMetadata) wrappedLoadFunc).getSchema(location, job);
        } else {
            return null;
        }
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException {
        if (wrappedLoadFunc instanceof LoadMetadata) {
            return ((LoadMetadata) wrappedLoadFunc).getStatistics(location, job);
        } else {
            return null;
        }
    }

    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException {
        if (wrappedLoadFunc instanceof LoadMetadata) {
            return ((LoadMetadata) wrappedLoadFunc).getPartitionKeys(location, job);
        } else {
            return null;
        }
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter) throws IOException {
        if (wrappedLoadFunc instanceof LoadMetadata) {
             ((LoadMetadata) wrappedLoadFunc).setPartitionFilter(partitionFilter);
        }
    }
    
    @Override
    public void setUDFContextSignature(String signature) {
        this.udfContextSignature = signature;
    }
}
