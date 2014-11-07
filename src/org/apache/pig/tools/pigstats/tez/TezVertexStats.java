package org.apache.pig.tools.pigstats.tez;

import static org.apache.pig.tools.pigstats.tez.TezDAGStats.FS_COUNTER_GROUP;
import static org.apache.pig.tools.pigstats.tez.TezDAGStats.PIG_COUNTER_GROUP;
import static org.apache.pig.tools.pigstats.tez.TezDAGStats.TASK_COUNTER_GROUP;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.pig.PigCounters;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
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
    // CounterGroup, Counter, Value
    private Map<String, Map<String, Long>> counters = null;

    private List<POStore> stores = null;
    private List<FileSpec> loads = null;

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
        sb.append(String.format("%1$20s: %2$-100s%n", "VertexName", name));
        if (getAlias() != null && !getAlias().isEmpty()) {
            sb.append(String.format("%1$20s: %2$-100s%n", "Alias", getAlias()));
        }
        if (getFeature() != null && !getFeature().isEmpty()) {
            sb.append(String.format("%1$20s: %2$-100s%n", "Features", getFeature()));
        }
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
                    conf.get("pig.inputs"));
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
        hdfsBytesRead = -1;
        hdfsBytesWritten = -1;

        if (status != null) {
            setSuccessful(status.getState().equals(VertexStatus.State.SUCCEEDED));
            this.parallelism = parallelism;
            if (this.isMapOpts) {
                numberMaps += parallelism;
            } else {
                numberReduces += parallelism;
            }
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

            if (counters.get(FS_COUNTER_GROUP) != null &&
                    counters.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_READ) != null) {
                hdfsBytesRead = counters.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_READ);
            }
            if (counters.get(FS_COUNTER_GROUP) != null &&
                    counters.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_WRITTEN) != null) {
                hdfsBytesWritten = counters.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_WRITTEN);
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
        if (loads == null) {
            return;
        }

        for (FileSpec fs : loads) {
            long records = -1;
            long hdfsBytesRead = -1;
            String filename = fs.getFileName();
            if (counters != null) {
                Map<String, Long> taskCounter = counters.get(TASK_COUNTER_GROUP);
                if (taskCounter != null
                        && taskCounter.get(TaskCounter.INPUT_RECORDS_PROCESSED.name()) != null) {
                    records = taskCounter.get(TaskCounter.INPUT_RECORDS_PROCESSED.name());
                    if (this.isMapOpts) {
                        mapInputRecords += records;
                    } else {
                        reduceInputRecords += records;
                    }
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
        if (stores == null) {
            return;
        }

        for (POStore sto : stores) {
            if (sto.isTmpStore()) {
                continue;
            }
            long records = -1;
            long hdfsBytesWritten = -1;
            String filename = sto.getSFile().getFileName();
            if (counters != null) {
                if (sto.isMultiStore()) {
                    Map<String, Long> msGroup = counters.get(PigStatsUtil.MULTI_STORE_COUNTER_GROUP);
                    if (msGroup != null) {
                        multiStoreCounters.putAll(msGroup);
                        Long n = msGroup.get(PigStatsUtil.getMultiStoreCounterName(sto));
                        if (n != null) records = n;
                    }
                } else if (counters.get(TASK_COUNTER_GROUP) != null
                        && counters.get(TASK_COUNTER_GROUP).get(TaskCounter.OUTPUT_RECORDS.name()) != null) {
                    records = counters.get(TASK_COUNTER_GROUP).get(TaskCounter.OUTPUT_RECORDS.name());
                }
                if (records != -1) {
                    if (this.isMapOpts) {
                        mapOutputRecords += records;
                    } else {
                        reduceOutputRecords += records;
                    }
                }
            }
            /* TODO: Need to check FILE_BYTES_WRITTEN for local mode */
            if (!sto.isMultiStore() && counters.get(FS_COUNTER_GROUP)!= null &&
                    counters.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_WRITTEN) != null) {
                hdfsBytesWritten = counters.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_WRITTEN);
            } else {
                hdfsBytesWritten = JobStats.getOutputSize(sto, conf);
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
        return numberMaps;
    }

    @Override
    @Deprecated
    public int getNumberReduces() {
        return numberReduces;
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
        return mapInputRecords;
    }

    @Override
    @Deprecated
    public long getMapOutputRecords() {
        return mapOutputRecords;
    }

    @Override
    @Deprecated
    public long getReduceInputRecords() {
        return reduceInputRecords;
    }

    @Override
    @Deprecated
    public long getReduceOutputRecords() {
        return reduceOutputRecords;
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
        return multiStoreCounters;
    }

    @Override
    @Deprecated
    public Map<String, Long> getMultiInputCounters() {
        throw new UnsupportedOperationException();
    }

}
