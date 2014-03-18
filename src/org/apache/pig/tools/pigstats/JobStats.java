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

package org.apache.pig.tools.pigstats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;

/**
 * This class encapsulates the runtime statistics of a MapReduce job.
 * Job statistics is collected when job is completed.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class JobStats extends Operator {

    public static final String ALIAS = "JobStatistics:alias";
    public static final String ALIAS_LOCATION = "JobStatistics:alias_location";
    public static final String FEATURE = "JobStatistics:feature";

    public static final String SUCCESS_HEADER = null;
    public static final String FAILURE_HEADER = null;

    public static enum JobState { UNKNOWN, SUCCESS, FAILED; }

    protected JobState state = JobState.UNKNOWN;

    protected ArrayList<OutputStats> outputs;

    protected ArrayList<InputStats> inputs;

    protected Configuration conf;

    protected long hdfsBytesRead = 0;
    protected long hdfsBytesWritten = 0;

    private String errorMsg;

    private Exception exception = null;

    protected JobStats(String name, JobGraph plan) {
        super(name, plan);
        outputs = new ArrayList<OutputStats>();
        inputs = new ArrayList<InputStats>();
    }

    public abstract String getJobId();

    public void setConf(Configuration conf) {
        if (conf == null) {
            return;
        }
        this.conf = conf;
    }

    public JobState getState() { return state; }

    public boolean isSuccessful() { return (state == JobState.SUCCESS); }

    public void setSuccessful(boolean isSuccessful) {
        this.state = isSuccessful ? JobState.SUCCESS : JobState.FAILED;
    }

    public String getErrorMessage() { return errorMsg; }

    public Exception getException() { return exception; }

    public List<OutputStats> getOutputs() {
        return Collections.unmodifiableList(outputs);
    }

    public List<InputStats> getInputs() {
        return Collections.unmodifiableList(inputs);
    }

    public String getAlias() {
        return (String)getAnnotation(ALIAS);
    }

    public String getAliasLocation() {
        return (String)getAnnotation(ALIAS_LOCATION);
    }

    public String getFeature() {
        return (String)getAnnotation(FEATURE);
    }

    public long getHdfsBytesRead() {
        return hdfsBytesRead;
    }

    public long getHdfsBytesWritten() {
        return hdfsBytesWritten;
    }

    /**
     * Returns the total bytes written to user specified HDFS
     * locations of this job.
     */
    public long getBytesWritten() {
        long count = 0;
        for (OutputStats out : outputs) {
            long n = out.getBytes();
            if (n > 0) count += n;
        }
        return count;
    }

    /**
     * Returns the total number of records in user specified output
     * locations of this job.
     */
    public long getRecordWrittern() {
        long count = 0;
        for (OutputStats out : outputs) {
            long rec = out.getNumberRecords();
            if (rec > 0) count += rec;
        }
        return count;
    }

    @Override
    public abstract void accept(PlanVisitor v) throws FrontendException;


    @Override
    public boolean isEqual(Operator operator) {
        if (!(operator instanceof JobStats)) return false;
        return name.equalsIgnoreCase(operator.getName());
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public void setBackendException(Exception e) {
        exception = e;
    }

    public abstract String getDisplayString(boolean isLocal);


    /**
     * Calculate the median value from the given array
     * @param durations
     * @return median value
     */
    protected long calculateMedianValue(long[] durations) {
        long median;
        // figure out the median
        Arrays.sort(durations);
        int midPoint = durations.length /2;
        if ((durations.length & 1) == 1) {
            // odd
            median = durations[midPoint];
        } else {
            // even
            median = (durations[midPoint-1] + durations[midPoint]) / 2;
        }
        return median;
    }

    public boolean isSampler() {
        return getFeature().contains(ScriptState.PIG_FEATURE.SAMPLER.name());
    }

    public boolean isIndexer() {
        return getFeature().contains(ScriptState.PIG_FEATURE.INDEXER.name());
    }

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getNumberMaps} instead.
     */
    @Deprecated
    abstract public int getNumberMaps();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getNumberReduces} instead.
     */
    @Deprecated
    abstract public int getNumberReduces();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getMaxMapTime} instead.
     */
    @Deprecated
    abstract public long getMaxMapTime();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getMinMapTime} instead.
     */
    @Deprecated
    abstract public long getMinMapTime();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getAvgMapTime} instead.
     */
    @Deprecated
    abstract public long getAvgMapTime();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getMaxReduceTime} instead.
     */
    @Deprecated
    abstract public long getMaxReduceTime();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getMinReduceTime} instead.
     */
    @Deprecated
    abstract public long getMinReduceTime();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getAvgREduceTime} instead.
     */
    @Deprecated
    abstract public long getAvgReduceTime();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getMapInputRecords} instead.
     */
    @Deprecated
    abstract public long getMapInputRecords();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getMapOutputRecords} instead.
     */
    @Deprecated
    abstract public long getMapOutputRecords();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getReduceInputRecords} instead.
     */
    @Deprecated
    abstract public long getReduceInputRecords();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getReduceOutputRecords} instead.
     */
    @Deprecated
    abstract public long getReduceOutputRecords();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getSMMSpillCount} instead.
     */
    @Deprecated
    abstract public long getSMMSpillCount();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getProactiveSpillCountObjects} instead.
     */
    @Deprecated
    abstract public long getProactiveSpillCountObjects();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getProactiveSpillCountRecs} instead.
     */
    @Deprecated
    abstract public long getProactiveSpillCountRecs();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getHadoopCounters} instead.
     */
    @Deprecated
    abstract public Counters getHadoopCounters();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getMultiStoreCounters} instead.
     */
    @Deprecated
    abstract public Map<String, Long> getMultiStoreCounters();

    /**
     * @deprecated If you are using mapreduce, please cast JobStats to org.apache.pig.tools.pigstats.mapreduce.MRJobStats,
     * then use {@link org.apache.pig.tools.pigstats.mapreduce.MRJobStats#getMultiInputCounters} instead.
     */
    @Deprecated
    abstract public Map<String, Long> getMultiInputCounters();

}
