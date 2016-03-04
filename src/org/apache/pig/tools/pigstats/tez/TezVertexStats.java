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
package org.apache.pig.tools.pigstats.tez;

import static org.apache.pig.tools.pigstats.tez.TezDAGStats.FS_COUNTER_GROUP;
import static org.apache.pig.tools.pigstats.tez.TezDAGStats.PIG_COUNTER_GROUP;
import static org.apache.pig.tools.pigstats.tez.TezDAGStats.TASK_COUNTER_GROUP;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.pig.PigCounters;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.PigStats.JobGraphPrinter;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.VertexStatus.State;

import com.google.common.collect.Maps;

/*
 * TezVertexStats encapsulates the statistics collected from a Tez Vertex.
 * It includes status of the execution as well as
 * information about outputs and inputs of the Vertex.
 */
public class TezVertexStats extends JobStats {
    private static final Log LOG = LogFactory.getLog(TezVertexStats.class);

    private boolean isMapOpts;
    private int parallelism;
    private State vertexState;
    // CounterGroup, Counter, Value
    private Map<String, Map<String, Long>> counters = null;

    private List<POStore> stores = null;
    private List<FileSpec> loads = null;

    private int numTasks = 0;
    private long numInputRecords = 0;
    private long numReduceInputRecords = 0;
    private long numOutputRecords = 0;
    private long fileBytesRead = 0;
    private long fileBytesWritten = 0;
    private long spillCount = 0;
    private long activeSpillCountObj = 0;
    private long activeSpillCountRecs = 0;

    private Map<String, Long> multiInputCounters = Maps.newHashMap();
    private Map<String, Long> multiStoreCounters = Maps.newHashMap();

    public TezVertexStats(String name, JobGraph plan, boolean isMapOpts) {
        super(name, plan);
        this.isMapOpts = isMapOpts;
    }

    @Override
    public String getJobId() {
        return name;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (v instanceof JobGraphPrinter) {
            JobGraphPrinter jpp = (JobGraphPrinter)v;
            jpp.visit(this);
        }
    }

    @Override
    public String getDisplayString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-10s ", name));
        if (state == JobState.FAILED) {
            sb.append(vertexState.name());
        }
        sb.append(String.format("%9s ", parallelism));
        sb.append(String.format("%10s ", numTasks));
        sb.append(String.format("%14s ", numInputRecords));
        sb.append(String.format("%20s ", numReduceInputRecords));
        sb.append(String.format("%14s ", numOutputRecords));
        sb.append(String.format("%14s ", fileBytesRead));
        sb.append(String.format("%16s ", fileBytesWritten));
        sb.append(String.format("%14s ", hdfsBytesRead));
        sb.append(String.format("%16s ", hdfsBytesWritten));
        sb.append(getAlias()).append("\t");
        sb.append(getFeature()).append("\t");
        for (OutputStats os : outputs) {
            sb.append(os.getLocation()).append(",");
        }
        sb.append("\n");
        return sb.toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setConf(Configuration conf) {
        super.setConf(conf);
        try {
            // TODO: We should replace PIG_REDUCE_STORES with something else in
            // tez. For now, we keep it since it's used in PigOutputFormat.
            this.stores = (List<POStore>) ObjectSerializer.deserialize(
                    conf.get(JobControlCompiler.PIG_REDUCE_STORES));
            this.loads = (List<FileSpec>) ObjectSerializer.deserialize(
                    conf.get(PigInputFormat.PIG_INPUTS));
        } catch (IOException e) {
            LOG.warn("Failed to deserialize the store list", e);
        }
    }

    public boolean hasLoadOrStore() {
        if ((loads != null && !loads.isEmpty())
                || (stores != null && !stores.isEmpty())) {
            return true;
        }
        return false;
    }

    public void accumulateStats(VertexStatus status, int parallelism) {

        if (status != null) {
            setSuccessful(status.getState().equals(VertexStatus.State.SUCCEEDED));
            this.vertexState = status.getState();
            this.parallelism = parallelism; //compile time parallelism
            this.numTasks = status.getProgress().getTotalTaskCount(); //run time parallelism
            TezCounters tezCounters = status.getVertexCounters();
            counters = Maps.newHashMap();
            Iterator<CounterGroup> grpIt = tezCounters.iterator();
            while (grpIt.hasNext()) {
                CounterGroup grp = grpIt.next();
                Iterator<TezCounter> cntIt = grp.iterator();
                Map<String, Long> cntMap = Maps.newHashMap();
                while (cntIt.hasNext()) {
                    TezCounter cnt = cntIt.next();
                    cntMap.put(cnt.getName(), cnt.getValue());
                }
                counters.put(grp.getName(), cntMap);
            }

            Map<String, Long> fsCounters = counters.get(FS_COUNTER_GROUP);
            if (fsCounters != null) {
                if (fsCounters.containsKey(PigStatsUtil.HDFS_BYTES_READ)) {
                    this.hdfsBytesRead = fsCounters.get(PigStatsUtil.HDFS_BYTES_READ);
                }
                if (fsCounters.containsKey(PigStatsUtil.HDFS_BYTES_WRITTEN)) {
                    this.hdfsBytesWritten = fsCounters.get(PigStatsUtil.HDFS_BYTES_WRITTEN);
                }
                if (fsCounters.containsKey(PigStatsUtil.FILE_BYTES_READ)) {
                    this.fileBytesRead = fsCounters.get(PigStatsUtil.FILE_BYTES_READ);
                }
                if (fsCounters.containsKey(PigStatsUtil.FILE_BYTES_WRITTEN)) {
                    this.fileBytesWritten = fsCounters.get(PigStatsUtil.FILE_BYTES_WRITTEN);
                }
            }

            Map<String, Long> pigCounters = counters.get(PIG_COUNTER_GROUP);
            if (pigCounters != null) {
                if (pigCounters.containsKey(PigCounters.SPILLABLE_MEMORY_MANAGER_SPILL_COUNT)) {
                    spillCount = pigCounters.get(PigCounters.SPILLABLE_MEMORY_MANAGER_SPILL_COUNT);
                }
                if (pigCounters.containsKey(PigCounters.PROACTIVE_SPILL_COUNT_BAGS)) {
                    activeSpillCountObj = pigCounters.get(PigCounters.PROACTIVE_SPILL_COUNT_BAGS);
                }
                if (pigCounters.containsKey(PigCounters.PROACTIVE_SPILL_COUNT_RECS)) {
                    activeSpillCountRecs = pigCounters.get(PigCounters.PROACTIVE_SPILL_COUNT_RECS);
                }
            }

            addInputStatistics();
            addOutputStatistics();
        }
    }

    public Map<String, Map<String, Long>> getCounters() {
        return counters;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void addInputStatistics() {

        long inputRecords = -1;
        Map<String, Long> taskCounters = counters.get(TASK_COUNTER_GROUP);
        if (taskCounters != null) {
            if (taskCounters.get(TaskCounter.INPUT_RECORDS_PROCESSED.name()) != null) {
                inputRecords = taskCounters.get(TaskCounter.INPUT_RECORDS_PROCESSED.name());
                numInputRecords = inputRecords;
            }
            if (taskCounters.get(TaskCounter.REDUCE_INPUT_RECORDS.name()) != null) {
                numReduceInputRecords = taskCounters.get(TaskCounter.REDUCE_INPUT_RECORDS.name());
            }
        }

        if (loads == null) {
            return;
        }

        Map<String, Long> mIGroup = counters.get(PigStatsUtil.MULTI_INPUTS_COUNTER_GROUP);
        if (mIGroup != null) {
            multiInputCounters.putAll(mIGroup);
        }

        // There is always only one load in a Tez vertex
        for (FileSpec fs : loads) {
            long records = -1;
            long hdfsBytesRead = -1;
            String filename = fs.getFileName();
            if (counters != null) {
                if (mIGroup != null) {
                    Long n = mIGroup.get(PigStatsUtil.getMultiInputsCounterName(fs.getFileName(), 0));
                    if (n != null) records = n;
                }
                if (records == -1) {
                    records = inputRecords;
                }
                if (isSuccessful() && records == -1) {
                    // Tez removes 0 value counters for efficiency.
                    records = 0;
                }
                if (counters.get(FS_COUNTER_GROUP) != null &&
                        counters.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_READ) != null) {
                    hdfsBytesRead = counters.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_READ);
                }
            }
            InputStats is = new InputStats(filename, hdfsBytesRead,
                    records, (state == JobState.SUCCESS));
            is.setConf(conf);
            inputs.add(is);
        }
    }

    public void addOutputStatistics() {

        long outputRecords = -1;

        Map<String, Long> taskCounters = counters.get(TASK_COUNTER_GROUP);
        if (taskCounters != null
                && taskCounters.get(TaskCounter.OUTPUT_RECORDS.name()) != null) {
            outputRecords = taskCounters.get(TaskCounter.OUTPUT_RECORDS.name());
            numOutputRecords = outputRecords;
        }

        if (stores == null) {
            return;
        }

        Map<String, Long> msGroup = counters.get(PigStatsUtil.MULTI_STORE_COUNTER_GROUP);
        if (msGroup != null) {
            multiStoreCounters.putAll(msGroup);
        }

        for (POStore sto : stores) {
            if (sto.isTmpStore()) {
                continue;
            }
            long records = -1;
            long hdfsBytesWritten = -1;
            String filename = sto.getSFile().getFileName();
            if (counters != null) {
                if (msGroup != null) {
                    Long n = msGroup.get(PigStatsUtil.getMultiStoreCounterName(sto));
                    if (n != null) records = n;
                }
                if (records == -1) {
                    records = outputRecords;
                }
                if (isSuccessful() && records == -1) {
                    // Tez removes 0 value counters for efficiency.
                    records = 0;
                }
            }
            /* TODO: Need to check FILE_BYTES_WRITTEN for local mode */
            if (!sto.isMultiStore() && counters.get(FS_COUNTER_GROUP)!= null &&
                    counters.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_WRITTEN) != null) {
                hdfsBytesWritten = counters.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_WRITTEN);
            } else {
                try {
                    hdfsBytesWritten = JobStats.getOutputSize(sto, conf);
                } catch (Exception e) {
                    LOG.warn("Error while getting the bytes written for the output " + sto.getSFile(), e);
                }
            }

            OutputStats os = new OutputStats(filename, hdfsBytesWritten,
                    records, (state == JobState.SUCCESS));
            os.setPOStore(sto);
            os.setConf(conf);
            outputs.add(os);
        }
    }

    @Override
    @Deprecated
    public int getNumberMaps() {
        return this.isMapOpts ? numTasks : -1;
    }

    @Override
    @Deprecated
    public int getNumberReduces() {
        return this.isMapOpts ? -1 : numTasks;
    }

    @Override
    @Deprecated
    public long getMaxMapTime() {
        return -1;
    }

    @Override
    @Deprecated
    public long getMinMapTime() {
        return -1;
    }

    @Override
    @Deprecated
    public long getAvgMapTime() {
        return -1;
    }

    @Override
    @Deprecated
    public long getMaxReduceTime() {
        return -1;
    }

    @Override
    @Deprecated
    public long getMinReduceTime() {
        return -1;
    }

    @Override
    @Deprecated
    public long getAvgREduceTime() {
        return -1;
    }

    @Override
    @Deprecated
    public long getMapInputRecords() {
        return this.isMapOpts ? numInputRecords : -1;
    }

    @Override
    @Deprecated
    public long getMapOutputRecords() {
        return this.isMapOpts ? numOutputRecords : -1;
    }

    @Override
    @Deprecated
    public long getReduceInputRecords() {
        return this.isMapOpts ? -1 : numInputRecords;
    }

    @Override
    @Deprecated
    public long getReduceOutputRecords() {
        return this.isMapOpts ? -1 : numOutputRecords;
    }

    @Override
    public long getSMMSpillCount() {
        return spillCount;
    }

    @Override
    public long getProactiveSpillCountObjects() {
        return activeSpillCountObj;
    }

    @Override
    public long getProactiveSpillCountRecs() {
        return activeSpillCountRecs;
    }

    @Override
    @Deprecated
    public Counters getHadoopCounters() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Map<String, Long> getMultiStoreCounters() {
        return Collections.unmodifiableMap(multiStoreCounters);
    }

    @Override
    @Deprecated
    public Map<String, Long> getMultiInputCounters() {
        throw new UnsupportedOperationException();
    }

}
