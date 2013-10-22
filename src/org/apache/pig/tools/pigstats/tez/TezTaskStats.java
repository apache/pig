package org.apache.pig.tools.pigstats.tez;

import java.util.Map;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.PigStats.JobGraphPrinter;

public class TezTaskStats extends JobStats {
    private ApplicationId appId;

    TezTaskStats(String name, JobGraph plan) {
        super(name, plan);
    }

    public void setId(ApplicationId appId) {
        this.appId = appId;
    }

    @Override
    public String getJobId() {
        return (appId == null) ? null : appId.toString();
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
        // TODO: what do we want to print for vertex statistics?
        return "";
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
