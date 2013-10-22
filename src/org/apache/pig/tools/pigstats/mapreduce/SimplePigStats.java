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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.NativeMapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;
import org.apache.pig.tools.pigstats.JobStats.JobState;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.JobStats;

/**
 * SimplePigStats encapsulates the statistics collected from a running script. 
 * It includes status of the execution, the DAG of its MR jobs, as well as 
 * information about outputs and inputs of the script. 
 */
public final class SimplePigStats extends PigStats {
    private static final Log LOG = LogFactory.getLog(SimplePigStats.class);

    private JobClient jobClient;
    private JobControlCompiler jcc;
    private Map<Job, MapReduceOper> jobMroMap;
    private Map<MapReduceOper, MRJobStats> mroJobMap;

    // successful jobs so far
    private Set<Job> jobSeen = new HashSet<Job>();
    private Map<String, OutputStats> aliasOuputMap;

    /**
     * This class builds the job DAG from a MR plan
     */
    private class JobGraphBuilder extends MROpPlanVisitor {

        public JobGraphBuilder(MROperPlan plan) {
            super(plan, new DependencyOrderWalker<MapReduceOper, MROperPlan>(
                    plan));
            jobPlan = new JobGraph();
            mroJobMap = new HashMap<MapReduceOper, MRJobStats>();        
        }
        
        @Override
        public void visitMROp(MapReduceOper mr) throws VisitorException {
            MRJobStats js = new MRJobStats(
                    mr.getOperatorKey().toString(), jobPlan);            
            jobPlan.add(js);
            List<MapReduceOper> preds = getPlan().getPredecessors(mr);
            if (preds != null) {
                for (MapReduceOper pred : preds) {
                    MRJobStats jpred = mroJobMap.get(pred);
                    if (!jobPlan.isConnected(jpred, js)) {
                        jobPlan.connect(jpred, js);
                    }
                }
            }
            mroJobMap.put(mr, js);            
        }        
    }
    
    @Override
    public List<String> getAllErrorMessages() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, List<PigStats>> getAllStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmbedded() {
        return false;
    }

    @Override
    public List<String> getOutputLocations() {
        ArrayList<String> locations = new ArrayList<String>();
        for (OutputStats output : getOutputStats()) {
            locations.add(output.getLocation());
        }
        return Collections.unmodifiableList(locations);
    }
 
    @Override
    public List<String> getOutputNames() {
        ArrayList<String> names = new ArrayList<String>();
        for (OutputStats output : getOutputStats()) {            
            names.add(output.getName());
        }
        return Collections.unmodifiableList(names);
    }
 
    @Override
    public long getNumberBytes(String location) {
        if (location == null) return -1;
        String name = new Path(location).getName();
        long count = -1;
        for (OutputStats output : getOutputStats()) {
            if (name.equals(output.getName())) {
                count = output.getBytes();
                break;
            }
        }
        return count;
    }

    @Override
    public long getNumberRecords(String location) {
        if (location == null) return -1;
        String name = new Path(location).getName();
        long count = -1;
        for (OutputStats output : getOutputStats()) {
            if (name.equals(output.getName())) {
                count = output.getNumberRecords();
                break;
            }
        }
        return count;
    }
 
    @Override
    public String getOutputAlias(String location) {
        if (location == null) return null;
        String name = new Path(location).getName();
        String alias = null;
        for (OutputStats output : getOutputStats()) {
            if (name.equals(output.getName())) {
                alias = output.getAlias();
                break;
            }
        }
        return alias;
    }

    @Override
    public long getSMMSpillCount() {
        Iterator<JobStats> it = jobPlan.iterator();
        long ret = 0;
        while (it.hasNext()) {
            ret += ((MRJobStats) it.next()).getSMMSpillCount();
        }
        return ret;
    }

    @Override
    public long getProactiveSpillCountObjects() {
        Iterator<JobStats> it = jobPlan.iterator();
        long ret = 0;
        while (it.hasNext()) {            
            ret += ((MRJobStats) it.next()).getProactiveSpillCountObjects();
        }
        return ret;
    }
    
    @Override
    public long getProactiveSpillCountRecords() {
        Iterator<JobStats> it = jobPlan.iterator();
        long ret = 0;
        while (it.hasNext()) {            
            ret += ((MRJobStats) it.next()).getProactiveSpillCountRecs();
        }
        return ret;
    }
    
    @Override
    public long getBytesWritten() {
        Iterator<JobStats> it = jobPlan.iterator();
        long ret = 0;
        while (it.hasNext()) {
            long n = it.next().getBytesWritten();
            if (n > 0) ret += n;
        }
        return ret;
    }
    
    @Override
    public long getRecordWritten() {
        Iterator<JobStats> it = jobPlan.iterator();
        long ret = 0;
        while (it.hasNext()) {
            long n = it.next().getRecordWrittern();
            if (n > 0) ret += n;
        }
        return ret;
    }

    @Override
    public List<OutputStats> getOutputStats() {
        List<OutputStats> outputs = new ArrayList<OutputStats>();
        Iterator<JobStats> iter = jobPlan.iterator();
        while (iter.hasNext()) {
            for (OutputStats os : iter.next().getOutputs()) {
                outputs.add(os);
            }
        }        
        return Collections.unmodifiableList(outputs);       
    }
    
    @Override
    public OutputStats result(String alias) {
        if (aliasOuputMap == null) {
            aliasOuputMap = new HashMap<String, OutputStats>();
            Iterator<JobStats> iter = jobPlan.iterator();
            while (iter.hasNext()) {
                for (OutputStats os : iter.next().getOutputs()) {
                    String a = os.getAlias();
                    if (a == null || a.length() == 0) {
                        LOG.warn("Output alias isn't avalable for " + os.getLocation());
                        continue;
                    }
                    aliasOuputMap.put(a, os);
                }
            }    
        }
        return aliasOuputMap.get(alias);
    }
    
    @Override
    public List<InputStats> getInputStats() {
        List<InputStats> inputs = new ArrayList<InputStats>();
        Iterator<JobStats> iter = jobPlan.iterator();
        while (iter.hasNext()) {
            for (InputStats is : iter.next().getInputs()) {
                inputs.add(is);
            }
        }        
        return Collections.unmodifiableList(inputs);       
    }
    
    public SimplePigStats() {        
        jobMroMap = new HashMap<Job, MapReduceOper>(); 
        jobPlan = new JobGraph();
    }
    
    void start(PigContext pigContext, JobClient jobClient, 
            JobControlCompiler jcc, MROperPlan mrPlan) {
        
        if (pigContext == null || jobClient == null || jcc == null) {
            LOG.warn("invalid params: " + pigContext + jobClient + jcc);
            return;
        }
        
        this.pigContext = pigContext;
        this.jobClient = jobClient;
        this.jcc = jcc;         
        
        // build job DAG with job ids assigned to null 
        try {
            new JobGraphBuilder(mrPlan).visit();
        } catch (VisitorException e) {
            LOG.warn("unable to build job plan", e);
        }
        
        startTime = System.currentTimeMillis();
        userId = System.getProperty("user.name");
    }
    
    void stop() {
        endTime = System.currentTimeMillis();
        int m = getNumberSuccessfulJobs();
        int n = getNumberFailedJobs();
 
        if (n == 0 && m > 0 && m == jobPlan.size()) {
            returnCode = ReturnCode.SUCCESS;
        } else if (m > 0 && m < jobPlan.size()) {
            returnCode = ReturnCode.PARTIAL_FAILURE;
        } else {
            returnCode = ReturnCode.FAILURE;
        }
    }
    
    boolean isInitialized() {
        return startTime > 0;
    }

    @Override
    public JobClient getJobClient() {
        return jobClient;
    }
    
    JobControlCompiler getJobControlCompiler() {
        return jcc;
    }
        
    @SuppressWarnings("deprecation")
    MRJobStats addMRJobStats(Job job) {
        MapReduceOper mro = jobMroMap.get(job);
         
        if (mro == null) {
            LOG.warn("unable to get MR oper for job: " + job.toString());
            return null;
        }
        MRJobStats js = mroJobMap.get(mro);
        
        JobID jobId = job.getAssignedJobID();
        js.setId(jobId);
        js.setAlias(mro);
        js.setConf(job.getJobConf());
        return js;
    }
    
    @SuppressWarnings("deprecation")
    public MRJobStats addMRJobStatsForNative(NativeMapReduceOper mr) {
        MRJobStats js = mroJobMap.get(mr);
        js.setId(new JobID(mr.getJobId(), NativeMapReduceOper.getJobNumber())); 
        js.setAlias(mr);
        
        return js;
    }
            
    void display() {
        if (returnCode == ReturnCode.UNKNOWN) {
            LOG.warn("unknown return code, can't display the results");
            return;
        }
        if (pigContext == null) {
            LOG.warn("unknown exec type, don't display the results");
            return;
        }
 
        // currently counters are not working in local mode - see PIG-1286
        ExecType execType = pigContext.getExecType();
        if (execType.isLocal()) {
            LOG.info("Detected Local mode. Stats reported below may be incomplete");
        }
        
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        StringBuilder sb = new StringBuilder();
        sb.append("\nHadoopVersion\tPigVersion\tUserId\tStartedAt\tFinishedAt\tFeatures\n");
        sb.append(getHadoopVersion()).append("\t").append(getPigVersion()).append("\t")
            .append(userId).append("\t")
            .append(sdf.format(new Date(startTime))).append("\t")
            .append(sdf.format(new Date(endTime))).append("\t")
            .append(getFeatures()).append("\n");
        sb.append("\n");
        if (returnCode == ReturnCode.SUCCESS) {
            sb.append("Success!\n");
        } else if (returnCode == ReturnCode.PARTIAL_FAILURE) {
            sb.append("Some jobs have failed! Stop running all dependent jobs\n");
        } else {
            sb.append("Failed!\n");
        }
        sb.append("\n");
                
        if (returnCode == ReturnCode.SUCCESS 
                || returnCode == ReturnCode.PARTIAL_FAILURE) {            
            sb.append("Job Stats (time in seconds):\n");
            if (execType.isLocal()) {
                sb.append(MRJobStats.SUCCESS_HEADER_LOCAL).append("\n");
            } else {
                sb.append(MRJobStats.SUCCESS_HEADER).append("\n");
            }
            List<JobStats> arr = jobPlan.getSuccessfulJobs();
            for (JobStats js : arr) {                
                sb.append(js.getDisplayString(execType.isLocal()));
            }
            sb.append("\n");
        }
        if (returnCode == ReturnCode.FAILURE
                || returnCode == ReturnCode.PARTIAL_FAILURE) {
            sb.append("Failed Jobs:\n");
            sb.append(MRJobStats.FAILURE_HEADER).append("\n");
            List<JobStats> arr = jobPlan.getFailedJobs();
            for (JobStats js : arr) {   
                sb.append(js.getDisplayString(execType.isLocal()));
            }
            sb.append("\n");
        }
        sb.append("Input(s):\n");
        for (InputStats is : getInputStats()) {
            sb.append(is.getDisplayString(execType.isLocal()));
        }
        sb.append("\n");
        sb.append("Output(s):\n");
        for (OutputStats ds : getOutputStats()) {
            sb.append(ds.getDisplayString(execType.isLocal()));
        }
        
        if (!(execType.isLocal())) {
            sb.append("\nCounters:\n");
            sb.append("Total records written : " + getRecordWritten()).append("\n");
            sb.append("Total bytes written : " + getBytesWritten()).append("\n");
            sb.append("Spillable Memory Manager spill count : "
                    + getSMMSpillCount()).append("\n");
            sb.append("Total bags proactively spilled: " 
                    + getProactiveSpillCountObjects()).append("\n");
            sb.append("Total records proactively spilled: " 
                    + getProactiveSpillCountRecords()).append("\n");
        }
        
        sb.append("\nJob DAG:\n").append(jobPlan.toString());
        
        LOG.info("Script Statistics: \n" + sb.toString());
    }
    
    void mapMROperToJob(MapReduceOper mro, Job job) {
        if (mro == null) {
            LOG.warn("null MR operator");
        } else {
            MRJobStats js = mroJobMap.get(mro);
            if (js == null) {
                LOG.warn("null job stats for mro: " + mro.getOperatorKey());
            } else {
                jobMroMap.put(job, mro);
            }
        }
    }   
    
    void setBackendException(Job job, Exception e) {
        if (e instanceof PigException) {
            LOG.error("ERROR " + ((PigException)e).getErrorCode() + ": " 
                    + e.getLocalizedMessage());
        } else if (e != null) {
            LOG.error("ERROR: " + e.getLocalizedMessage());
        }
        
        if (job.getAssignedJobID() == null || e == null) {
            LOG.debug("unable to set backend exception");
            return;
        }
        String id = job.getAssignedJobID().toString();
        Iterator<JobStats> iter = jobPlan.iterator();
        while (iter.hasNext()) {
            JobStats js = iter.next();
            if (id.equals(js.getJobId())) {
                js.setBackendException(e);
                break;
            }
        }
    }
    
    PigContext getPigContext() {
        return pigContext;
    }
    
    int getNumberSuccessfulJobs() {
        Iterator<JobStats> iter = jobPlan.iterator();
        int count = 0;
        while (iter.hasNext()) {
            if (iter.next().getState() == JobState.SUCCESS) count++; 
        }
        return count;
    }
    
    int getNumberFailedJobs() {
        Iterator<JobStats> iter = jobPlan.iterator();
        int count = 0;
        while (iter.hasNext()) {
            if (iter.next().getState() == JobState.FAILED) count++; 
        }
        return count;
    }
    
    boolean isJobSeen(Job job) {
        return !jobSeen.add(job);    
    }
    
}
