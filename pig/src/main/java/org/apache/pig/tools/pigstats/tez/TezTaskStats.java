package org.apache.pig.tools.pigstats.tez;

import static org.apache.pig.tools.pigstats.tez.TezStats.FS_COUNTER_GROUP;
import static org.apache.pig.tools.pigstats.tez.TezStats.TASK_COUNTER_GROUP;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
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
import org.apache.tez.common.counters.TaskCounter;

public class TezTaskStats extends JobStats {
    private static final Log LOG = LogFactory.getLog(TezTaskStats.class);

    private String vertexName;
    private List<POStore> stores = null;
    private List<FileSpec> loads = null;

    public TezTaskStats(String name, JobGraph plan) {
        super(name, plan);
    }

    public void setId(String vertexName) {
        this.vertexName = vertexName;
    }

    @Override
    public String getJobId() {
        return (vertexName == null) ? "" : vertexName;
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
        sb.append(String.format("%1$20s: %2$-100s%n", "VertexName", vertexName));
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

    public void addInputStatistics(Map<String, Map<String, Long>> map) {
        if (loads == null) {
            return;
        }

        for (FileSpec fs : loads) {
            long records = -1;
            long hdfsBytesRead = -1;
            String filename = fs.getFileName();
            if (map != null) {
                Map<String, Long> taskCounter = map.get(TASK_COUNTER_GROUP);
                if (taskCounter != null
                        && taskCounter.get(TaskCounter.INPUT_RECORDS_PROCESSED.name()) != null) {
                    records = taskCounter.get(TaskCounter.INPUT_RECORDS_PROCESSED.name());
                }
                if (map.get(FS_COUNTER_GROUP) !=null &&
                        map.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_READ) != null) {
                    hdfsBytesRead = map.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_READ);
                }
            }
            InputStats is = new InputStats(filename, hdfsBytesRead,
                    records, (state == JobState.SUCCESS));
            is.setConf(conf);
            inputs.add(is);
        }
    }

    public void addOutputStatistics(Map<String, Map<String, Long>> map) {
        if (stores == null) {
            return;
        }

        for (POStore sto : stores) {
            long records = -1;
            long hdfsBytesWritten = JobStats.getOutputSize(sto, conf);
            String filename = sto.getSFile().getFileName();
            if (map != null) {
                if (sto.isMultiStore()) {
                    Map<String, Long> msGroup = map.get(PigStatsUtil.MULTI_STORE_COUNTER_GROUP);
                    Long n = msGroup == null ? null: msGroup.get(PigStatsUtil.getMultiStoreCounterName(sto));
                    if (n != null) records = n;
                } else if (map.get(TASK_COUNTER_GROUP) != null
                        && map.get(TASK_COUNTER_GROUP).get(TaskCounter.OUTPUT_RECORDS.name()) != null) {
                    records = map.get(TASK_COUNTER_GROUP).get(TaskCounter.OUTPUT_RECORDS.name());
                }
            }
            /*
            if (map.get(FS_COUNTER_GROUP)!= null &&
                    map.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_WRITTEN) != null) {
                hdfsBytesWritten = map.get(FS_COUNTER_GROUP).get(PigStatsUtil.HDFS_BYTES_WRITTEN);
            }
            */
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
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public int getNumberReduces() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getMaxMapTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getMinMapTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getAvgMapTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getMaxReduceTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getMinReduceTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getAvgREduceTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getMapInputRecords() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getMapOutputRecords() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getReduceInputRecords() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getReduceOutputRecords() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getSMMSpillCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getProactiveSpillCountObjects() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getProactiveSpillCountRecs() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Counters getHadoopCounters() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Map<String, Long> getMultiStoreCounters() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Map<String, Long> getMultiInputCounters() {
        throw new UnsupportedOperationException();
    }

    public boolean hasLoadOrStore() {
        if ((loads != null && !loads.isEmpty())
                || (stores != null && !stores.isEmpty())) {
            return true;
        }
        return false;
    }
}
