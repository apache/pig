package org.apache.pig.tools.pigstats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SparkStats extends PigStats {
    private List<OutputStats> outputStatsList = new ArrayList<OutputStats>();
    private JobGraph jobGraph = new JobGraph();

    public void addOutputInfo(POStore poStore, long totalBytes,
            long totalRecords, boolean success, Configuration conf) {
        OutputStats outputStats = new OutputStats(poStore.getSFile()
                .getFileName(), totalBytes, totalRecords, success);
        outputStats.setPOStore(poStore);
        outputStats.setConf(conf);
        outputStatsList.add(outputStats);
    }

    @Override
    public boolean isSuccessful() {
        for (OutputStats output : outputStatsList) {
            if (!output.isSuccessful()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public JobClient getJobClient() {
        return null;
    }

    @Override
    public boolean isEmbedded() {
        return false;
    }

    @Override
    public Map<String, List<PigStats>> getAllStats() {
        return null;
    }

    @Override
    public List<String> getAllErrorMessages() {
        return null;
    }

    @Override
    public Properties getPigProperties() {
        return null;
    }

    @Override
    public JobGraph getJobGraph() {
        return jobGraph;
    }

    @Override
    public List<String> getOutputLocations() {
        return null;
    }

    @Override
    public List<String> getOutputNames() {
        return null;
    }

    @Override
    public long getNumberBytes(String location) {
        return 0;
    }

    @Override
    public long getNumberRecords(String location) {
        return 0;
    }

    @Override
    public String getOutputAlias(String location) {
        return null;
    }

    @Override
    public long getSMMSpillCount() {
        return 0;
    }

    @Override
    public long getProactiveSpillCountObjects() {
        return 0;
    }

    @Override
    public long getProactiveSpillCountRecords() {
        return 0;
    }

    @Override
    public long getBytesWritten() {
        return 0;
    }

    @Override
    public long getRecordWritten() {
        return 0;
    }

    @Override
    public String getScriptId() {
        return null;
    }

    @Override
    public String getFeatures() {
        return null;
    }

    @Override
    public long getDuration() {
        return 0;
    }

    @Override
    public int getNumberJobs() {
        return 0;
    }

    @Override
    public List<OutputStats> getOutputStats() {
        return outputStatsList;
    }

    @Override
    public OutputStats result(String alias) {
        return null;
    }

    @Override
    public List<InputStats> getInputStats() {
        return null;
    }
}
