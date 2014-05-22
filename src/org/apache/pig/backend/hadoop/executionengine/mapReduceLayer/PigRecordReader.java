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

import static org.apache.pig.PigConfiguration.TIME_UDFS;
import static org.apache.pig.PigConfiguration.TIME_UDFS_FREQUENCY;
import static org.apache.pig.PigConstants.TIME_UDFS_ELAPSED_TIME_COUNTER;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
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

    transient private String counterGroup = "";
    private long timingFrequency = 100L;
    private boolean doTiming = false;

    /**
     * the current Tuple value as returned by underlying
     * {@link LoadFunc#getNext()}
     */
    Tuple curValue = null;

    // the current wrapped RecordReader used by the loader
    private RecordReader<?, ?> curReader;

    // the loader object
    private LoadFunc loadfunc;

    // the Hadoop counter name
    transient private String counterName = null;

    // the wrapped inputformat
    private InputFormat<?, ?> inputformat;

    // the wrapped splits
    private PigSplit pigSplit;

    // the wrapped split index in use
    private int idx;

    private long progress;

    private TaskAttemptContext context;

    private PigStatusReporter reporter;

    private final long limit;

    private long recordCount = 0;

    /**
     * the Configuration object with data specific to the input the underlying
     * RecordReader will process (this is obtained after a
     * {@link LoadFunc#setLocation(String, org.apache.hadoop.mapreduce.Job)}
     * call and hence can contain specific properties the underlying
     * {@link InputFormat} might have put in.
     */
    private Configuration inputSpecificConf;
    /**
     * @param context
     *
     */
    public PigRecordReader(InputFormat<?, ?> inputformat, PigSplit pigSplit,
            LoadFunc loadFunc, TaskAttemptContext context, long limit) throws IOException, InterruptedException {
        this.inputformat = inputformat;
        this.pigSplit = pigSplit;
        this.loadfunc = loadFunc;
        this.context = context;
        this.reporter = PigStatusReporter.getInstance();
        this.inputSpecificConf = context.getConfiguration();
        curReader = null;
        progress = 0;
        idx = 0;
        this.limit = limit;
        initNextRecordReader();
        doTiming = inputSpecificConf.getBoolean(TIME_UDFS, false);
        if (doTiming) {
            counterGroup = loadFunc.toString();
            timingFrequency = inputSpecificConf.getLong(TIME_UDFS_FREQUENCY, 100L);
        }
    }

    @Override
    public void close() throws IOException {
        if (curReader != null) {
            curReader.close();
            curReader = null;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        // In pig we don't really use the key in the input to the map - so send
        // null
        return null;
    }

    @Override
    public Tuple getCurrentValue() throws IOException, InterruptedException {
        // Increment the multi-input record counter
        if (counterName != null && curValue != null) {
            reporter.incrCounter(PigStatsUtil.MULTI_INPUTS_COUNTER_GROUP, counterName, 1);
        }
        return curValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        long subprogress = 0;    // bytes processed in current split
        if (null != curReader) {
            // idx is always one past the current subsplit's true index.
            subprogress = (long)(curReader.getProgress() * pigSplit.getLength(idx - 1));
        }
        return Math.min(1.0f,  (progress + subprogress)/(float)(pigSplit.getLength()));
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // initialize the underlying actual RecordReader with the right Context
        // object - this is achieved by merging the Context corresponding to
        // the input split this Reader is supposed to process with the context
        // passed in.
        this.pigSplit = (PigSplit)split;
        this.context = context;
        ConfigurationUtil.mergeConf(context.getConfiguration(),
                inputSpecificConf);
        // Pass loader signature to LoadFunc and to InputFormat through
        // the conf
        PigInputFormat.passLoadSignature(loadfunc, pigSplit.getInputIndex(),
                context.getConfiguration());
        // now invoke initialize() on underlying RecordReader with
        // the "adjusted" conf
        if (null != curReader) {
            curReader.initialize(pigSplit.getWrappedSplit(), context);
            loadfunc.prepareToRead(curReader, pigSplit);
        }
        if (pigSplit.isMultiInputs() && !pigSplit.disableCounter()) {
            counterName = getMultiInputsCounerName(pigSplit, inputSpecificConf);
            if (counterName != null) {
                // Create the counter. This is needed because incrCounter() may
                // never be called in case of empty file.
                reporter.incrCounter(PigStatsUtil.MULTI_INPUTS_COUNTER_GROUP, counterName, 0);
            }
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (limit != -1 && recordCount >= limit)
            return false;
        boolean timeThis = doTiming && ( (recordCount + 1) % timingFrequency == 0);
        long startNanos = 0;
        if (timeThis) {
            startNanos = System.nanoTime();
        }
        while ((curReader == null) || (curValue = loadfunc.getNext()) == null) {
            if (!initNextRecordReader()) {
              return false;
            }
        }
        if (timeThis) {
            reporter.incrCounter(counterGroup, TIME_UDFS_ELAPSED_TIME_COUNTER,
                    Math.round((System.nanoTime() - startNanos) / 1000) * timingFrequency);
        }
        recordCount++;
        return true;
    }

    @SuppressWarnings("unchecked")
    private static String getMultiInputsCounerName(PigSplit pigSplit,
            Configuration conf) throws IOException {
        ArrayList<FileSpec> inputs =
            (ArrayList<FileSpec>) ObjectSerializer.deserialize(
                    conf.get(PigInputFormat.PIG_INPUTS));
        String fname = inputs.get(pigSplit.getInputIndex()).getFileName();
        return PigStatsUtil.getMultiInputsCounterName(fname, pigSplit.getInputIndex());
    }

    /**
     * Get the record reader for the next chunk in this CombineFileSplit.
     */
    protected boolean initNextRecordReader() throws IOException, InterruptedException {

        if (curReader != null) {
            curReader.close();
            curReader = null;
            if (idx > 0) {
                progress += pigSplit.getLength(idx-1);    // done processing so far
            }
            context.progress();
        }

        // if all chunks have been processed, nothing more to do.
        if (idx == pigSplit.getNumPaths()) {
            return false;
        }

        // get a record reader for the idx-th chunk
        try {

            pigSplit.setCurrentIdx(idx);
            curReader =  inputformat.createRecordReader(pigSplit.getWrappedSplit(), context);
            LOG.info("Current split being processed "+pigSplit.getWrappedSplit());

            if (idx > 0) {
                // initialize() for the first RecordReader will be called by MapTask;
                // we're responsible for initializing subsequent RecordReaders.
                curReader.initialize(pigSplit.getWrappedSplit(), context);
                loadfunc.prepareToRead(curReader, pigSplit);
            }
        } catch (Exception e) {
            throw new RuntimeException (e);
        }
        idx++;
        return true;
    }
}
