package org.apache.pig.tools.pigstats.spark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.spark.JobMetricsListener;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SparkPigStats extends PigStats {

    private static final Log LOG = LogFactory.getLog(SparkPigStats.class);

    public SparkPigStats() {
      jobPlan = new JobGraph();
    }

    public void addJobStats(POStore poStore, int jobId,
                            JobMetricsListener jobMetricsListener,
                            JavaSparkContext sparkContext,
                            Configuration conf) {
        boolean isSuccess = SparkStatsUtil.isJobSuccess(jobId, sparkContext);
        SparkJobStats jobStats = new SparkJobStats(jobId, jobPlan);
        jobStats.setSuccessful(isSuccess);
        jobStats.addOutputInfo(poStore, isSuccess, jobMetricsListener, conf);
        jobStats.collectStats(jobMetricsListener);
        jobPlan.add(jobStats);
    }

    public void addFailJobStats(POStore poStore, String jobId,
                                JobMetricsListener jobMetricsListener,
                                JavaSparkContext sparkContext,
                                Configuration conf,
                                Exception e) {
        boolean isSuccess = false;
        SparkJobStats jobStats = new SparkJobStats(jobId, jobPlan);
        jobStats.setSuccessful(isSuccess);
        jobStats.addOutputInfo(poStore, isSuccess, jobMetricsListener, conf);
        jobStats.collectStats(jobMetricsListener);
        jobPlan.add(jobStats);
        if (e != null) {
            jobStats.setBackendException(e);
        }
    }

    public void finish() {
        super.stop();
        display();
    }

    private void display() {
        Iterator<JobStats> iter = jobPlan.iterator();
        while (iter.hasNext()) {
            SparkJobStats js = (SparkJobStats)iter.next();
            LOG.info( "Spark Job [" + js.getJobId() + "] Metrics");
            Map<String, Long> stats = js.getStats();
            if (stats == null) {
                LOG.info("No statistics found for job " + js.getJobId());
                return;
            }

            Iterator statIt = stats.entrySet().iterator();
            while (statIt.hasNext()) {
                Map.Entry pairs = (Map.Entry)statIt.next();
                LOG.info("\t" + pairs.getKey() + " : " + pairs.getValue());
            }
        }
    }

    @Override
    public JobClient getJobClient() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmbedded() {
        return false;
    }

    @Override
    public Map<String, List<PigStats>> getAllStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getAllErrorMessages() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Properties getPigProperties() {
        return null;
    }

    @Override
    public String getOutputAlias(String location) {
        // TODO
        return null;
    }

    @Override
    public long getSMMSpillCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getProactiveSpillCountObjects() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getProactiveSpillCountRecords() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBytesWritten() {
        // TODO
        return 0;
    }

    @Override
    public long getRecordWritten() {
        // TODO
        return 0;
    }

    @Override
    public int getNumberJobs() {
        return jobPlan.size();
    }

    @Override
    public OutputStats result(String alias) {
        return null;
    }
}
