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
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.pig.PigCounters;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.newplan.PlanVisitor;
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
            "MaxMapTime\tMinMapTime\tAvgMapTime\tMedianMapTime\tMaxReduceTime\t" +
            "MinReduceTime\tAvgReduceTime\tMedianReducetime\tAlias\tFeature\tOutputs";

    public static final String FAILURE_HEADER = "JobId\tAlias\tFeature\tMessage\tOutputs";

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

    @Override
    public String getJobId() {
        return (jobId == null) ? null : jobId.toString();
    }

    @Override
    public int getNumberMaps() { return numberMaps; }

    @Override
    public int getNumberReduces() { return numberReduces; }

    @Override
    public long getMaxMapTime() { return maxMapTime; }

    @Override
    public long getMinMapTime() { return minMapTime; }

    @Override
    public long getAvgMapTime() { return avgMapTime; }

    @Override
    public long getMaxReduceTime() { return maxReduceTime; }

    @Override
    public long getMinReduceTime() { return minReduceTime; }

    @Override
    public long getAvgREduceTime() { return avgReduceTime; }

    @Override
    public long getMapInputRecords() { return mapInputRecords; }

    @Override
    public long getMapOutputRecords() { return mapOutputRecords; }

    @Override
    public long getReduceInputRecords() { return reduceInputRecords; }

    @Override
    public long getReduceOutputRecords() { return reduceOutputRecords; }

    @Override
    public long getSMMSpillCount() { return spillCount; }

    @Override
    public long getProactiveSpillCountObjects() { return activeSpillCountObj; }

    @Override
    public long getProactiveSpillCountRecs() { return activeSpillCountRecs; }

    @Override
    public Counters getHadoopCounters() { return counters; }

    @Override
    public Map<String, Long> getMultiStoreCounters() {
        return Collections.unmodifiableMap(multiStoreCounters);
    }

    @Override
    public Map<String, Long> getMultiInputCounters() {
        return Collections.unmodifiableMap(multiInputCounters);
    }

    @Override
    public String getAlias() {
        return (String)getAnnotation(ALIAS);
    }

    @Override
    public String getAliasLocation() {
        return (String)getAnnotation(ALIAS_LOCATION);
    }

    @Override
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

    private static void appendStat(long stat, StringBuilder sb) {
        if(stat != -1) {
            sb.append(stat/1000);
        } else {
            sb.append("n/a");
        }
        sb.append("\t");
    }

    @Override
    public String getDisplayString() {
        StringBuilder sb = new StringBuilder();
        String id = (jobId == null) ? "N/A" : jobId.toString();
        if (state == JobState.FAILED) {
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
            appendStat(maxMapTime, sb);
            appendStat(minMapTime, sb);
            appendStat(avgMapTime, sb);
            appendStat(medianMapTime, sb);
            appendStat(maxReduceTime, sb);
            appendStat(minReduceTime, sb);
            appendStat(avgReduceTime, sb);
            appendStat(medianReduceTime, sb);

            sb.append(getAlias()).append("\t")
                .append(getFeature()).append("\t");
        }
        for (OutputStats os : outputs) {
            sb.append(os.getLocation()).append(",");
        }
        sb.append("\n");
        return sb.toString();
    }

    void addCounters(Job job) {
        try {
            counters = HadoopShims.getCounters(job);
        } catch (IOException e) {
            LOG.warn("Unable to get job counters", e);
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

    private class TaskStat {
        int size;
        long max;
        long min;
        long avg;
        long median;

        public TaskStat(int size, long max, long min, long avg, long median) {
            this.size = size;
            this.max = max;
            this.min = min;
            this.avg = avg;
            this.median = median;
        }
    }

    void addMapReduceStatistics(Job job) {
        Iterator<TaskReport> maps = null;
        try {
            maps = HadoopShims.getTaskReports(job, TaskType.MAP);
        } catch (IOException e) {
            LOG.warn("Failed to get map task report", e);
        }
        Iterator<TaskReport> reduces = null;
        try {
            reduces = HadoopShims.getTaskReports(job, TaskType.REDUCE);
        } catch (IOException e) {
            LOG.warn("Failed to get reduce task report", e);
        }
        addMapReduceStatistics(maps, reduces);
    }

    private TaskStat getTaskStat(Iterator<TaskReport> tasks) {
        int size = 0;
        long max = 0;
        long min = Long.MAX_VALUE;
        long median = 0;
        long total = 0;
        List<Long> durations = new ArrayList<Long>();

        while(tasks.hasNext()){
            TaskReport rpt = tasks.next();
            long duration = rpt.getFinishTime() - rpt.getStartTime();
            durations.add(duration);
            max = (duration > max) ? duration : max;
            min = (duration < min) ? duration : min;
            total += duration;
            size++;
        }
        long avg = total / size;

        median = calculateMedianValue(durations);

        return new TaskStat(size, max, min, avg, median);
    }

    private void addMapReduceStatistics(Iterator<TaskReport> maps, Iterator<TaskReport> reduces) {
        if (maps != null && maps.hasNext()) {
            TaskStat st = getTaskStat(maps);
            setMapStat(st.size, st.max, st.min, st.avg, st.median);
        } else {
            int m = conf.getInt(MRConfiguration.MAP_TASKS, 1);
            if (m > 0) {
                setMapStat(m, -1, -1, -1, -1);
            }
        }

        if (reduces != null && reduces.hasNext()) {
            TaskStat st = getTaskStat(reduces);
            setReduceStat(st.size, st.max, st.min, st.avg, st.median);
        } else {
            int m = conf.getInt(MRConfiguration.REDUCE_TASKS, 1);
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
