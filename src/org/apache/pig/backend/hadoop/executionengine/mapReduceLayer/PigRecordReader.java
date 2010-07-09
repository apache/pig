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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.tools.pigstats.PigStatusReporter;

/**
 * A wrapper around the actual RecordReader and loadfunc - this is needed for
 * two reasons
 * 1) To intercept the initialize call from hadoop and initialize the underlying
 * actual RecordReader with the right Context object - this is achieved by 
 * looking up the Context corresponding to the input split this Reader is 
 * supposed to process
 * 2) We need to give hadoop consistent key-value types - text and tuple 
 * respectively - so PigRecordReader will call underlying Loader's getNext() to
 * get the Tuple value - the key is null text since key is not used in input to
 * map() in Pig.
 */
public class PigRecordReader extends RecordReader<Text, Tuple> {

    private static final Log LOG = LogFactory.getLog(PigRecordReader.class);
    /**
     * the current Tuple value as returned by underlying
     * {@link LoadFunc#getNext()}
     */
    Tuple curValue = null;
    
    // the underlying RecordReader used by the loader
    @SuppressWarnings("unchecked")
    private RecordReader wrappedReader;
    
    // the loader object
    private LoadFunc loadfunc;
    
    // the Hadoop counter for multi-input jobs 
    transient private Counter inputRecordCounter = null;
    
    // the Hadoop counter name
    transient private String counterName = null;
    
    /**
     * the Configuration object with data specific to the input the underlying
     * RecordReader will process (this is obtained after a 
     * {@link LoadFunc#setLocation(String, org.apache.hadoop.mapreduce.Job)} 
     * call and hence can contain specific properties the underlying
     * {@link InputFormat} might have put in.
     */
    private Configuration inputSpecificConf;
    /**
     * @param conf 
     * 
     */
    public PigRecordReader(RecordReader wrappedReader, 
            LoadFunc loadFunc, Configuration conf) {
        this.wrappedReader = wrappedReader; 
        this.loadfunc = loadFunc;
        this.inputSpecificConf = conf;
    }
    
    @Override
    public void close() throws IOException {
        wrappedReader.close();        
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        // In pig we don't really use the key in the input to the map - so send
        // null
        return null;
    }

    @Override
    public Tuple getCurrentValue() throws IOException, InterruptedException {    
        if (inputRecordCounter == null && counterName != null) {
            PigStatusReporter reporter = PigStatusReporter.getInstance();
            if (reporter != null) {
                inputRecordCounter = reporter.getCounter(
                        PigStatsUtil.MULTI_INPUTS_COUNTER_GROUP,
                        counterName);
                LOG.info("Created input record counter: " + counterName);
            } else {
                LOG.warn("Get null reporter for " + counterName);
            }
        }
        // Increment the multi-input record counter
        if (inputRecordCounter != null && curValue != null) {
            inputRecordCounter.increment(1);            
        }
       
        return curValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return wrappedReader.getProgress();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // initialize the underlying actual RecordReader with the right Context 
        // object - this is achieved by merging the Context corresponding to 
        // the input split this Reader is supposed to process with the context
        // passed in.
        PigSplit pigSplit = (PigSplit)split;
        ConfigurationUtil.mergeConf(context.getConfiguration(),
                inputSpecificConf);
        // Pass loader signature to LoadFunc and to InputFormat through
        // the conf
        PigInputFormat.passLoadSignature(loadfunc, pigSplit.getInputIndex(), 
                context.getConfiguration());
        // now invoke initialize() on underlying RecordReader with
        // the "adjusted" conf
        wrappedReader.initialize(pigSplit.getWrappedSplit(), context);
        loadfunc.prepareToRead(wrappedReader, pigSplit);
                
        if (pigSplit.isMultiInputs()) { 
            counterName = getMultiInputsCounerName(pigSplit, inputSpecificConf);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        curValue = loadfunc.getNext();
        return curValue != null;
    }

    @SuppressWarnings("unchecked")
    private static String getMultiInputsCounerName(PigSplit pigSplit,
            Configuration conf) throws IOException {
        ArrayList<FileSpec> inputs = 
            (ArrayList<FileSpec>) ObjectSerializer.deserialize(
                    conf.get(PigInputFormat.PIG_INPUTS));
        String fname = inputs.get(pigSplit.getInputIndex()).getFileName();
        return PigStatsUtil.getMultiInputsCounterName(fname);
    }
}
