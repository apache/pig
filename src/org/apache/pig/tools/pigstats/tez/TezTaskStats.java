package org.apache.pig.tools.pigstats.tez;

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

public class TezTaskStats extends JobStats {
    private static final Log LOG = LogFactory.getLog(TezTaskStats.class);

    private String vertexName;
    private Configuration conf;
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
    public String getDisplayString(boolean isLocal) {
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

    public void addInputStatistics() {
        if (inputs == null) {
            LOG.warn("Unable to get inputs of the job");
            return;
        }

        for (FileSpec fs : loads) {
            String filename = fs.getFileName();
            // TODO: Records and bytesRead are always -1 now. We should update
            // them when Tez supports counters.
            long records = -1;
            long hdfsBytesRead = -1;
            InputStats is = new InputStats(filename, hdfsBytesRead,
                    records, (state == JobState.SUCCESS));
            is.setConf(conf);
            inputs.add(is);
        }
    }

    public void addOutputStatistics() {
        if (stores == null) {
            LOG.warn("Unable to get stores of the job");
            return;
        }

        for (POStore sto : stores) {
            String filename = sto.getSFile().getFileName();
            // TODO: Records and bytesRead are always -1 now. We should update
            // them when Tez supports counters.
            long records = -1;
            long hdfsBytesWritten = -1;
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
    public long getReduceOutputRecords() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public long getReduceInputRecords() {
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
    public long getHdfsBytesWritten() {
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
}
