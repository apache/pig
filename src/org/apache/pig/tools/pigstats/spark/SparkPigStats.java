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
package org.apache.pig.tools.pigstats.spark;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.spark.JobMetricsListener;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkPigStats extends PigStats {

    private Map<SparkJobStats,SparkOperator> jobSparkOperatorMap = new HashMap<SparkJobStats, SparkOperator>();
    private static final Log LOG = LogFactory.getLog(SparkPigStats.class);

    private SparkScriptState sparkScriptState;

    public SparkPigStats() {
        jobPlan = new JobGraph();
        this.sparkScriptState = (SparkScriptState) ScriptState.get();
    }

    public void initialize(PigContext pigContext, SparkOperPlan sparkPlan){
        super.start();
        this.pigContext = pigContext;
        sparkScriptState.setScriptInfo(sparkPlan);
    }

    public void addJobStats(POStore poStore, SparkOperator sparkOperator, int jobId,
                            JobMetricsListener jobMetricsListener,
                            JavaSparkContext sparkContext,
                            Configuration conf) {
        boolean isSuccess = SparkStatsUtil.isJobSuccess(jobId, sparkContext);
        SparkJobStats jobStats = new SparkJobStats(jobId, jobPlan);
        jobStats.setSuccessful(isSuccess);
        jobStats.addOutputInfo(poStore, isSuccess, jobMetricsListener, conf);
        jobStats.collectStats(jobMetricsListener);
        jobSparkOperatorMap.put(jobStats, sparkOperator);
        jobPlan.add(jobStats);
    }

    public void addFailJobStats(POStore poStore, SparkOperator sparkOperator, String jobId,
                                JobMetricsListener jobMetricsListener,
                                JavaSparkContext sparkContext,
                                Configuration conf,
                                Exception e) {
        boolean isSuccess = false;
        SparkJobStats jobStats = new SparkJobStats(jobId, jobPlan);
        jobStats.setSuccessful(isSuccess);
        jobStats.addOutputInfo(poStore, isSuccess, jobMetricsListener, conf);
        jobStats.collectStats(jobMetricsListener);
        jobSparkOperatorMap.put(jobStats, sparkOperator);
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
            if (jobSparkOperatorMap.containsKey(js)) {
                SparkOperator sparkOperator = jobSparkOperatorMap.get(js);
                js.setAlias(sparkOperator);
            }
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
    public int getNumberJobs() {
        return jobPlan.size();
    }

    @Override
    public OutputStats result(String alias) {
        return null;
    }
}
