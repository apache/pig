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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.JobMetricsListener;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.NativeSparkOperator;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkPigStats extends PigStats {

    private Map<SparkJobStats,SparkOperator> jobSparkOperatorMap = new HashMap<SparkJobStats, SparkOperator>();
    private static final Log LOG = LogFactory.getLog(SparkPigStats.class);

    private Set<SparkOperator> sparkOperatorsSet = new HashSet<SparkOperator>();

    private SparkScriptState sparkScriptState;

    private Configuration conf;

    public SparkPigStats() {
        jobPlan = new JobGraph();
        this.sparkScriptState = (SparkScriptState) ScriptState.get();
    }

    public void initialize(PigContext pigContext, SparkOperPlan sparkPlan, Configuration conf) {
        super.start();
        this.pigContext = pigContext;
        this.conf = conf;
        sparkScriptState.setScriptInfo(sparkPlan);
    }

    public void addJobStats(POStore poStore, SparkOperator sparkOperator, int jobId,
                            JobMetricsListener jobMetricsListener,
                            JavaSparkContext sparkContext) {
        boolean isSuccess = SparkStatsUtil.isJobSuccess(jobId, sparkContext);
        SparkJobStats jobStats = new SparkJobStats(jobId, jobPlan, conf);
        jobStats.setSuccessful(isSuccess);
        jobStats.collectStats(jobMetricsListener);
        jobStats.addOutputInfo(poStore, isSuccess, jobMetricsListener);
        addInputInfoForSparkOper(sparkOperator, jobStats, isSuccess, jobMetricsListener, conf);
        jobSparkOperatorMap.put(jobStats, sparkOperator);
        jobPlan.add(jobStats);
    }


    public void addFailJobStats(POStore poStore, SparkOperator sparkOperator, String jobId,
                                JobMetricsListener jobMetricsListener,
                                JavaSparkContext sparkContext,
                                Exception e) {
        boolean isSuccess = false;
        SparkJobStats jobStats = new SparkJobStats(jobId, jobPlan, conf);
        jobStats.setSuccessful(isSuccess);
        jobStats.collectStats(jobMetricsListener);
        jobStats.addOutputInfo(poStore, isSuccess, jobMetricsListener);
        addInputInfoForSparkOper(sparkOperator, jobStats, isSuccess, jobMetricsListener, conf);
        jobSparkOperatorMap.put(jobStats, sparkOperator);
        jobPlan.add(jobStats);
        if (e != null) {
            jobStats.setBackendException(e);
        }
    }

    public void addNativeJobStats(NativeSparkOperator sparkOperator, String jobId, boolean isSuccess, Exception e) {
        SparkJobStats jobStats = new SparkJobStats(jobId, jobPlan, conf);
        jobStats.setSuccessful(isSuccess);
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
       LOG.info(getDisplayString());
    }

    @Override
    public String getDisplayString() {
        StringBuilder sb = new StringBuilder();
        Iterator<JobStats> iter = jobPlan.iterator();
        while (iter.hasNext()) {
            SparkJobStats js = (SparkJobStats)iter.next();
            if (jobSparkOperatorMap.containsKey(js)) {
                SparkOperator sparkOperator = jobSparkOperatorMap.get(js);
                js.setAlias(sparkOperator);
            }
            sb.append("Spark Job [" + js.getJobId() + "] Metrics");
            Map<String, Long> stats = js.getStats();
            if (stats == null) {
                sb.append("No statistics found for job " + js.getJobId());
                return sb.toString();
            }

            Iterator statIt = stats.entrySet().iterator();
            while (statIt.hasNext()) {
                Map.Entry pairs = (Map.Entry)statIt.next();
                sb.append("\t" + pairs.getKey() + " : " + pairs.getValue());
            }
            for (InputStats inputStat : js.getInputs()){
                sb.append("\t"+inputStat.getDisplayString());
            }
        }
        return sb.toString();
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

    /**
     * SparkPlan can have many SparkOperators.
     * Each SparkOperator can have multiple POStores
     * We currently collect stats once for every POStore,
     * But do not want to collect input stats for every POStore
     *
     * e.g. After multiQuery optimization, the sparkOperator may look like this:
     * POLoad_1             (PhysicalPlan) ...POStore_A
     *         \          /
     *          ...POSplit
     *         /          \
     * POLoad_2            (PhysicalPlan) ...POStore_B
     */
    private void addInputInfoForSparkOper(SparkOperator sparkOperator,
                                          SparkJobStats jobStats,
                                          boolean isSuccess,
                                          JobMetricsListener jobMetricsListener,
                                          Configuration conf) {
        //to avoid repetition
        if (sparkOperatorsSet.contains(sparkOperator)) {
            return;
        }

        try {
            List<POLoad> poLoads = PlanHelper.getPhysicalOperators(sparkOperator.physicalPlan, POLoad.class);
            for (POLoad load : poLoads) {
                if (!load.isTmpLoad()) {
                    jobStats.addInputStats(load, isSuccess, (poLoads.size() == 1));
                }
            }
        } catch (VisitorException ve) {
            LOG.warn(ve);
        }

        sparkOperatorsSet.add(sparkOperator);
    }

}
