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

package org.apache.pig.tools.pigstats.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.pig.PigCounters;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.FileBasedOutputSizeReader;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigStatsOutputSizeReader;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.PigStats.JobGraphPrinter;


/**
 * This class encapsulates the runtime statistics of a MapReduce job.
 * Job statistics is collected when job is completed.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class MRJobStats extends JobStats {


    MRJobStats(String name, JobGraph plan) {
        super(name, plan);
    }

    public static final String SUCCESS_HEADER = "JobId\tMaps\tReduces\t" +
            "MaxMapTime\tMinMapTIme\tAvgMapTime\tMedianMapTime\tMaxReduceTime\t" +
            "MinReduceTime\tAvgReduceTime\tMedianReducetime\tAlias\tFeature\tOutputs";

    public static final String FAILURE_HEADER = "JobId\tAlias\tFeature\tMessage\tOutputs";

    // currently counters are not working in local mode - see PIG-1286
    public static final String SUCCESS_HEADER_LOCAL = "JobId\tAlias\tFeature\tOutputs";

    private static final Log LOG = LogFactory.getLog(MRJobStats.class);

    private List<POStore> mapStores = null;

    private List<POStore> reduceStores = null;

    private List<FileSpec> loads = null;

    private Boolean disableCounter = false;

    private JobID jobId;

    private long maxMapTime = 0;
    private long minMapTime = 0;
    private long avgMapTime = 0;
    private long medianMapTime = 0;
    private long maxReduceTime = 0;
    private long minReduceTime = 0;
    private long avgReduceTime = 0;
    private long medianReduceTime = 0;

    private int numberMaps = 0;
    private int numberReduces = 0;

    private long mapInputRecords = 0;
    private long mapOutputRecords = 0;
    private long reduceInputRecords = 0;
    private long reduceOutputRecords = 0;
    private long spillCount = 0;
    private long activeSpillCountObj = 0;
    private long activeSpillCountRecs = 0;

    private HashMap<String, Long> multiStoreCounters
            = new HashMap<String, Long>();

    private HashMap<String, Long> multiInputCounters
            = new HashMap<String, Long>();

    private Counters counters = null;

    public String getJobId() {
        return (jobId == null) ? null : jobId.toString();
    }

    public int getNumberMaps() { return numberMaps; }

    public int getNumberReduces() { return numberReduces; }

    public long getMaxMapTime() { return maxMapTime; }

    public long getMinMapTime() { return minMapTime; }

    public long getAvgMapTime() { return avgMapTime; }

    public long getMaxReduceTime() { return maxReduceTime; }

    public long getMinReduceTime() { return minReduceTime; }

    public long getAvgReduceTime() { return avgReduceTime; }

    public long getMapInputRecords() { return mapInputRecords; }

    public long getMapOutputRecords() { return mapOutputRecords; }

    public long getReduceInputRecords() { return reduceInputRecords; }

    public long getReduceOutputRecords() { return reduceOutputRecords; }

    public long getSMMSpillCount() { return spillCount; }

    public long getProactiveSpillCountObjects() { return activeSpillCountObj; }

    public long getProactiveSpillCountRecs() { return activeSpillCountRecs; }

    public Counters getHadoopCounters() { return counters; }

    public Map<String, Long> getMultiStoreCounters() {
        return Collections.unmodifiableMap(multiStoreCounters);
    }

    public Map<String, Long> getMultiInputCounters() {
        return Collections.unmodifiableMap(multiInputCounters);
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

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (v instanceof JobGraphPrinter) {
            JobGraphPrinter jpp = (JobGraphPrinter)v;
            jpp.visit(this);
        }
    }

    void setId(JobID jobId) {
        this.jobId = jobId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setConf(Configuration conf) {
        super.setConf(conf);
        try {
            this.mapStores = (List<POStore>) ObjectSerializer.deserialize(conf
                    .get(JobControlCompiler.PIG_MAP_STORES));
            this.reduceStores = (List<POStore>) ObjectSerializer.deserialize(conf
                    .get(JobControlCompiler.PIG_REDUCE_STORES));
            this.loads = (ArrayList<FileSpec>) ObjectSerializer.deserialize(conf
                    .get("pig.inputs"));
            this.disableCounter = conf.getBoolean("pig.disable.counter", false);
        } catch (IOException e) {
            LOG.warn("Failed to deserialize the store list", e);
        }
    }

    void setMapStat(int size, long max, long min, long avg, long median) {
        numberMaps = size;
        maxMapTime = max;
        minMapTime = min;
        avgMapTime = avg;
        medianMapTime = median;
    }

    void setReduceStat(int size, long max, long min, long avg, long median) {
        numberReduces = size;
        maxReduceTime = max;
        minReduceTime = min;
        avgReduceTime = avg;
        medianReduceTime = median;
    }

    public String getDisplayString(boolean local) {
        StringBuilder sb = new StringBuilder();
        String id = (jobId == null) ? "N/A" : jobId.toString();
        if (state == JobState.FAILED || local) {
            sb.append(id).append("\t")
                .append(getAlias()).append("\t")
                .append(getFeature()).append("\t");
            if (state == JobState.FAILED) {
                sb.append("Message: ").append(getErrorMessage()).append("\t");
            }
        } else if (state == JobState.SUCCESS) {
            sb.append(id).append("\t")
                .append(numberMaps).append("\t")
                .append(numberReduces).append("\t");
            if (numberMaps == 0) {
                sb.append("n/a\t").append("n/a\t").append("n/a\t").append("n/a\t");
            } else {
                sb.append(maxMapTime/1000).append("\t")
                    .append(minMapTime/1000).append("\t")
                    .append(avgMapTime/1000).append("\t")
                    .append(medianMapTime/1000).append("\t");
            }
            if (numberReduces == 0) {
                sb.append("n/a\t").append("n/a\t").append("n/a\t").append("n/a\t");
            } else {
                sb.append(maxReduceTime/1000).append("\t")
                    .append(minReduceTime/1000).append("\t")
                    .append(avgReduceTime/1000).append("\t")
                    .append(medianReduceTime/1000).append("\t");
            }
            sb.append(getAlias()).append("\t")
                .append(getFeature()).append("\t");
        }
        for (OutputStats os : outputs) {
            sb.append(os.getLocation()).append(",");
        }
        sb.append("\n");
        return sb.toString();
    }

    void addCounters(RunningJob rjob) {
        if (rjob != null) {
            try {
                counters = rjob.getCounters();
            } catch (IOException e) {
                LOG.warn("Unable to get job counters", e);
            }
        }
        if (counters != null) {
            Counters.Group taskgroup = counters
                    .getGroup(MRPigStatsUtil.TASK_COUNTER_GROUP);
            Counters.Group hdfsgroup = counters
                    .getGroup(MRPigStatsUtil.FS_COUNTER_GROUP);
            Counters.Group multistoregroup = counters
                    .getGroup(MRPigStatsUtil.MULTI_STORE_COUNTER_GROUP);
            Counters.Group multiloadgroup = counters
                    .getGroup(MRPigStatsUtil.MULTI_INPUTS_COUNTER_GROUP);

            mapInputRecords = taskgroup.getCounterForName(
                    MRPigStatsUtil.MAP_INPUT_RECORDS).getCounter();
            mapOutputRecords = taskgroup.getCounterForName(
                    MRPigStatsUtil.MAP_OUTPUT_RECORDS).getCounter();
            reduceInputRecords = taskgroup.getCounterForName(
                    MRPigStatsUtil.REDUCE_INPUT_RECORDS).getCounter();
            reduceOutputRecords = taskgroup.getCounterForName(
                    MRPigStatsUtil.REDUCE_OUTPUT_RECORDS).getCounter();
            hdfsBytesRead = hdfsgroup.getCounterForName(
                    MRPigStatsUtil.HDFS_BYTES_READ).getCounter();
            hdfsBytesWritten = hdfsgroup.getCounterForName(
                    MRPigStatsUtil.HDFS_BYTES_WRITTEN).getCounter();
            spillCount = counters.findCounter(
                    PigCounters.SPILLABLE_MEMORY_MANAGER_SPILL_COUNT)
                    .getCounter();
            activeSpillCountObj = counters.findCounter(
                    PigCounters.PROACTIVE_SPILL_COUNT_BAGS).getCounter();
            activeSpillCountRecs = counters.findCounter(
                    PigCounters.PROACTIVE_SPILL_COUNT_RECS).getCounter();

            Iterator<Counter> iter = multistoregroup.iterator();
            while (iter.hasNext()) {
                Counter cter = iter.next();
                multiStoreCounters.put(cter.getName(), cter.getValue());
            }

            Iterator<Counter> iter2 = multiloadgroup.iterator();
            while (iter2.hasNext()) {
                Counter cter = iter2.next();
                multiInputCounters.put(cter.getName(), cter.getValue());
            }

        }
    }

    void addMapReduceStatistics(JobClient client, Configuration conf) {
        TaskReport[] maps = null;
        try {
            maps = client.getMapTaskReports(jobId);
        } catch (IOException e) {
            LOG.warn("Failed to get map task report", e);
        }
        if (maps != null && maps.length > 0) {
            int size = maps.length;
            long max = 0;
            long min = Long.MAX_VALUE;
            long median = 0;
            long total = 0;
            long durations[] = new long[size];

            for (int i = 0; i < maps.length; i++) {
                TaskReport rpt = maps[i];
                long duration = rpt.getFinishTime() - rpt.getStartTime();
                durations[i] = duration;
                max = (duration > max) ? duration : max;
                min = (duration < min) ? duration : min;
                total += duration;
            }
            long avg = total / size;

            median = calculateMedianValue(durations);
            setMapStat(size, max, min, avg, median);
        } else {
            int m = conf.getInt("mapred.map.tasks", 1);
            if (m > 0) {
                setMapStat(m, -1, -1, -1, -1);
            }
        }

        TaskReport[] reduces = null;
        try {
            reduces = client.getReduceTaskReports(jobId);
        } catch (IOException e) {
            LOG.warn("Failed to get reduce task report", e);
        }
        if (reduces != null && reduces.length > 0) {
            int size = reduces.length;
            long max = 0;
            long min = Long.MAX_VALUE;
            long median = 0;
            long total = 0;
            long durations[] = new long[size];

            for (int i = 0; i < reduces.length; i++) {
                TaskReport rpt = reduces[i];
                long duration = rpt.getFinishTime() - rpt.getStartTime();
                durations[i] = duration;
                max = (duration > max) ? duration : max;
                min = (duration < min) ? duration : min;
                total += duration;
            }
            long avg = total / size;
            median = calculateMedianValue(durations);
            setReduceStat(size, max, min, avg, median);
        } else {
            int m = conf.getInt("mapred.reduce.tasks", 1);
            if (m > 0) {
                setReduceStat(m, -1, -1, -1, -1);
            }
        }
    }

    void setAlias(MapReduceOper mro) {
        MRScriptState ss = MRScriptState.get();
        annotate(ALIAS, ss.getAlias(mro));
        annotate(ALIAS_LOCATION, ss.getAliasLocation(mro));
        annotate(FEATURE, ss.getPigFeature(mro));
    }

    void addOutputStatistics() {
        if (mapStores == null || reduceStores == null) {
            LOG.warn("unable to get stores of the job");
            return;
        }

        if (mapStores.size() + reduceStores.size() == 1) {
            POStore sto = (mapStores.size() > 0) ? mapStores.get(0)
                    : reduceStores.get(0);
            if (!sto.isTmpStore()) {
                long records = (mapStores.size() > 0) ? mapOutputRecords
                        : reduceOutputRecords;
                OutputStats ds = new OutputStats(sto.getSFile().getFileName(),
                        hdfsBytesWritten, records, (state == JobState.SUCCESS));
                ds.setPOStore(sto);
                ds.setConf(conf);
                outputs.add(ds);

                if (state == JobState.SUCCESS) {
                     MRScriptState.get().emitOutputCompletedNotification(ds);
                }
            }
        } else {
            for (POStore sto : mapStores) {
                if (sto.isTmpStore()) continue;
                addOneOutputStats(sto);
            }
            for (POStore sto : reduceStores) {
                if (sto.isTmpStore()) continue;
                addOneOutputStats(sto);
            }
        }
    }

    /**
     * Looks up the output size reader from OUTPUT_SIZE_READER_KEY and invokes
     * it to get the size of output. If OUTPUT_SIZE_READER_KEY is not set,
     * defaults to FileBasedOutputSizeReader.
     * @param sto POStore
     * @param conf configuration
     */
    static long getOutputSize(POStore sto, Configuration conf) {
        PigStatsOutputSizeReader reader = null;
        String readerNames = conf.get(
                PigStatsOutputSizeReader.OUTPUT_SIZE_READER_KEY,
                FileBasedOutputSizeReader.class.getCanonicalName());

        for (String className : readerNames.split(",")) {
            reader = (PigStatsOutputSizeReader) PigContext.instantiateFuncFromSpec(className);
            if (reader.supports(sto)) {
                LOG.info("using output size reader: " + className);
                try {
                    return reader.getOutputSize(sto, conf);
                } catch (FileNotFoundException e) {
                    LOG.warn("unable to find the output file", e);
                    return -1;
                } catch (IOException e) {
                    LOG.warn("unable to get byte written of the job", e);
                    return -1;
                }
            }
        }

        LOG.warn("unable to find an output size reader");
        return -1;
    }

    private void addOneOutputStats(POStore sto) {
        long records = -1;
        if (sto.isMultiStore()) {
            Long n = multiStoreCounters.get(MRPigStatsUtil.getMultiStoreCounterName(sto));
            if (n != null) records = n;
        } else {
            records = mapOutputRecords;
        }

        long bytes = getOutputSize(sto, conf);
        String location = sto.getSFile().getFileName();
        OutputStats ds = new OutputStats(location, bytes, records,
                (state == JobState.SUCCESS));
        ds.setPOStore(sto);
        ds.setConf(conf);
        outputs.add(ds);

        if (state == JobState.SUCCESS) {
             MRScriptState.get().emitOutputCompletedNotification(ds);
        }
    }

    void addInputStatistics() {
        if (loads == null)  {
            LOG.warn("unable to get inputs of the job");
            return;
        }

        if (loads.size() == 1) {
            FileSpec fsp = loads.get(0);
            if (!MRPigStatsUtil.isTempFile(fsp.getFileName())) {
                long records = mapInputRecords;
                InputStats is = new InputStats(fsp.getFileName(),
                        hdfsBytesRead, records, (state == JobState.SUCCESS));
                is.setConf(conf);
                if (isSampler()) is.markSampleInput();
                if (isIndexer()) is.markIndexerInput();
                inputs.add(is);
            }
        } else {
            for (int i=0; i<loads.size(); i++) {
                FileSpec fsp = loads.get(i);
                if (MRPigStatsUtil.isTempFile(fsp.getFileName())) continue;
                addOneInputStats(fsp.getFileName(), i);
            }
        }
    }

    private void addOneInputStats(String fileName, int index) {
        long records = -1;
        Long n = multiInputCounters.get(
                MRPigStatsUtil.getMultiInputsCounterName(fileName, index));
        if (n != null) {
            records = n;
        } else {
            // the file could be empty
            if (!disableCounter) records = 0;
            else {
                LOG.warn("unable to get input counter for " + fileName);
            }
        }
        InputStats is = new InputStats(fileName, -1, records, (state == JobState.SUCCESS));
        is.setConf(conf);
        inputs.add(is);
    }

}
