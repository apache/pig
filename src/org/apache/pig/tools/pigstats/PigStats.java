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
package org.apache.pig.tools.pigstats;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.pig.PigException;
import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.NativeMapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.InternalCachedBag;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.SpillableMemoryManager;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.tools.pigstats.JobStats.JobState;

/**
 * PigStats encapsulates the statistics collected from a running script. 
 * It includes status of the execution, the DAG of its MR jobs, as well as 
 * information about outputs and inputs of the script. 
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class PigStats {
    
    private static final Log LOG = LogFactory.getLog(PigStats.class);
    
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";  
    
    private static ThreadLocal<PigStats> tps = new ThreadLocal<PigStats>();
    
    private PigContext pigContext;
    
    private JobClient jobClient;
    
    private JobControlCompiler jcc;
    
    private JobGraph jobPlan;
    
    // map MR job id to MapReduceOper
    private Map<String, MapReduceOper> jobMroMap;
     
    private Map<MapReduceOper, JobStats> mroJobMap;
      
    private long startTime = -1;
    private long endTime = -1;
    
    private String userId;
    
    private int returnCode = ReturnCode.UNKNOWN;
    private String errorMessage;
    private int errorCode = -1;
    
    public static PigStats get() {
        if (tps.get() == null) tps.set(new PigStats());
        return tps.get();
    }
        
    static PigStats start() {
        tps.set(new PigStats());
        return tps.get();
    }
    
    /**
     * JobGraph is an {@link OperatorPlan} whose members are {@link JobStats}
     */
    public static class JobGraph extends BaseOperatorPlan {
                
        @Override
        public String toString() {
            JobGraphPrinter jp = new JobGraphPrinter(this);
            try {
                jp.visit();
            } catch (FrontendException e) {
                LOG.warn("unable to print job plan", e);
            }
            return jp.toString();
        }
        
        public Iterator<JobStats> iterator() {
            return new Iterator<JobStats>() {
                private Iterator<Operator> iter = getOperators();                
                @Override
                public boolean hasNext() {                
                    return iter.hasNext();
                }
                @Override
                public JobStats next() {              
                    return (JobStats)iter.next();
                }
                @Override
                public void remove() {}
            };
        }
 
        boolean isConnected(Operator from, Operator to) {
            List<Operator> succs = null;
            succs = getSuccessors(from);
            if (succs != null) {
                for (Operator succ: succs) {
                    if (succ.getName().equals(to.getName()) 
                            || isConnected(succ, to)) {
                        return true;
                    }                    
                }
            }
            return false;
        }
        
        List<JobStats> getSuccessfulJobs() {
            ArrayList<JobStats> lst = new ArrayList<JobStats>();
            Iterator<JobStats> iter = iterator();
            while (iter.hasNext()) {
                JobStats js = iter.next();
                if (js.getState() == JobState.SUCCESS) {
                    lst.add(js);
                }
            }
            Collections.sort(lst, new JobComparator());
            return lst;
        }
        
        List<JobStats> getFailedJobs() {
            ArrayList<JobStats> lst = new ArrayList<JobStats>();
            Iterator<JobStats> iter = iterator();
            while (iter.hasNext()) {
                JobStats js = iter.next();
                if (js.getState() == JobState.FAILED) {
                    lst.add(js);
                }
            }            
            return lst;
        }
    }
    
    /**
     * This class builds the job DAG from a MR plan
     */
    private class JobGraphBuilder extends MROpPlanVisitor {

        public JobGraphBuilder(MROperPlan plan) {
            super(plan, new DependencyOrderWalker<MapReduceOper, MROperPlan>(
                    plan));
            jobPlan = new JobGraph();
            mroJobMap = new HashMap<MapReduceOper, JobStats>();        
        }
        
        @Override
        public void visitMROp(MapReduceOper mr) throws VisitorException {
            JobStats js = new JobStats(
                    mr.getOperatorKey().toString(), jobPlan);            
            jobPlan.add(js);
            List<MapReduceOper> preds = getPlan().getPredecessors(mr);
            if (preds != null) {
                for (MapReduceOper pred : preds) {
                    JobStats jpred = mroJobMap.get(pred);
                    if (!jobPlan.isConnected(jpred, js)) {
                        jobPlan.connect(jpred, js);
                    }
                }
            }
            mroJobMap.put(mr, js);            
        }        
    }
    
    /**
     * This class prints a JobGraph
     */
    static class JobGraphPrinter extends PlanVisitor {
        
        StringBuffer buf;

        protected JobGraphPrinter(OperatorPlan plan) {
            super(plan,
                    new org.apache.pig.newplan.DependencyOrderWalker(
                            plan));
            buf = new StringBuffer();
        }
        
        public void visit(JobStats op) throws FrontendException {
            buf.append(op.getJobId());
            List<Operator> succs = plan.getSuccessors(op);
            if (succs != null) {
                buf.append("\t->\t");
                for (Operator p : succs) {                  
                    buf.append(((JobStats)p).getJobId()).append(",");
                }               
            }
            buf.append("\n");
        }
        
        @Override
        public String toString() {
            buf.append("\n");
            return buf.toString();
        }        
    }
    
    private static class JobComparator implements Comparator<JobStats> {
        @Override
        public int compare(JobStats o1, JobStats o2) {           
            return o1.getJobId().compareTo(o2.getJobId());
        }       
    }
    
    public boolean isSuccessful() {
        return (returnCode == ReturnCode.SUCCESS);
    }
    
    /**
     * Return codes are defined in {@link ReturnCode}
     */
    public int getReturnCode() {
        return returnCode;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Returns the error code of {@link PigException}
     */
    public int getErrorCode() {
        return errorCode;
    }
    
    /**
     * Returns the DAG of the MR jobs spawned by the script
     */
    public JobGraph getJobGraph() {
        return jobPlan;
    }
    
    /**
     * Returns the list of output locations in the script
     */
    public List<String> getOutputLocations() {
        ArrayList<String> locations = new ArrayList<String>();
        for (OutputStats output : getOutputStats()) {
            locations.add(output.getLocation());
        }
        return Collections.unmodifiableList(locations);
    }
    
    /**
     * Returns the list of output names in the script
     */
    public List<String> getOutputNames() {
        ArrayList<String> names = new ArrayList<String>();
        for (OutputStats output : getOutputStats()) {            
            names.add(output.getName());
        }
        return Collections.unmodifiableList(names);
    }
    
    /**
     * Returns the number of bytes for the given output location,
     * -1 for invalid location or name.
     */
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
    
    /**
     * Returns the number of records for the given output location,
     * -1 for invalid location or name.
     */
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
        
    /**
     * Returns the alias associated with this output location
     */
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
    
    /**
     * Returns the total spill counts from {@link SpillableMemoryManager}.
     */
    public long getSMMSpillCount() {
        Iterator<JobStats> it = jobPlan.iterator();
        long ret = 0;
        while (it.hasNext()) {
            ret += it.next().getSMMSpillCount();
        }
        return ret;
    }
    
    /**
     * Returns the total number of bags that spilled proactively
     */
    public long getProactiveSpillCountObjects() {
        Iterator<JobStats> it = jobPlan.iterator();
        long ret = 0;
        while (it.hasNext()) {            
            ret += it.next().getProactiveSpillCountObjects();
        }
        return ret;
    }
    
    /**
     * Returns the total number of records that spilled proactively
     */
    public long getProactiveSpillCountRecords() {
        Iterator<JobStats> it = jobPlan.iterator();
        long ret = 0;
        while (it.hasNext()) {            
            ret += it.next().getProactiveSpillCountRecs();
        }
        return ret;
    }
    
    /**
     * Returns the total bytes written to user specified HDFS
     * locations of this script.
     */
    public long getBytesWritten() {
        Iterator<JobStats> it = jobPlan.iterator();
        long ret = 0;
        while (it.hasNext()) {
            long n = it.next().getBytesWritten();
            if (n > 0) ret += n;
        }
        return ret;
    }
    
    /**
     * Returns the total number of records in user specified output
     * locations of this script.
     */
    public long getRecordWritten() {
        Iterator<JobStats> it = jobPlan.iterator();
        long ret = 0;
        while (it.hasNext()) {
            long n = it.next().getRecordWrittern();
            if (n > 0) ret += n;
        }
        return ret;
    }

    public String getHadoopVersion() {
        return ScriptState.get().getHadoopVersion();
    }
    
    public String getPigVersion() {
        return ScriptState.get().getPigVersion();
    }
   
    public String getScriptId() {
        return ScriptState.get().getId();
    }
    
    public String getFeatures() {
        return ScriptState.get().getScriptFeatures();
    }
    
    public long getDuration() {
        return (startTime > 0 && endTime > 0) ? (endTime - startTime) : -1;
    }
    
    /**
     * Returns the number of MR jobs for this script
     */
    public int getNumberJobs() {
        return jobPlan.size();
    }
        
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
    
    private PigStats() {        
        jobMroMap = new HashMap<String, MapReduceOper>(); 
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
    
    JobClient getJobClient() {
        return jobClient;
    }
    
    JobControlCompiler getJobControlCompiler() {
        return jcc;
    }
    
    void setReturnCode(int returnCode) {
        this.returnCode = returnCode; 
    }
        
    @SuppressWarnings("deprecation")
    JobStats addJobStats(Job job) {
        MapReduceOper mro = null;
        JobID jobId = job.getAssignedJobID();
        if (jobId != null) {
            mro = jobMroMap.get(jobId.toString());
        } else {
            mro = jobMroMap.get(job.toString());
        }
        if (mro == null) {
            LOG.warn("unable to get MR oper for job: "
                    + ((jobId == null) ? job.toString() : jobId.toString()));
            return null;
        }
        JobStats js = mroJobMap.get(mro);
        
        js.setAlias(mro);
        js.setConf(job.getJobConf());
        return js;
    }
    
    @SuppressWarnings("deprecation")
    public JobStats addJobStatsForNative(NativeMapReduceOper mr) {
        JobStats js = mroJobMap.get(mr);
        js.setId(new JobID(mr.getJobId(), NativeMapReduceOper.getJobNumber())); 
        js.setAlias(mr);
        
        return js;
    }
            
    void display() {
        if (returnCode == ReturnCode.UNKNOWN) {
            LOG.warn("unknown return code, can't display the results");
            return;
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
            sb.append("JobId\tMaps\tReduces\tMaxMapTime\tMinMapTIme\t" +
                    "AvgMapTime\tMaxReduceTime\tMinReduceTime\tAvgReduceTime\t" +
                    "Alias\tFeature\tOutputs\n");
            List<JobStats> arr = jobPlan.getSuccessfulJobs();
            for (JobStats js : arr) {                
                sb.append(js.getDisplayString());
            }
            sb.append("\n");
        }
        if (returnCode == ReturnCode.FAILURE
                || returnCode == ReturnCode.PARTIAL_FAILURE) {
            sb.append("Failed Jobs:\n");
            sb.append("JobId\tAlias\tFeature\tMessage\tOutputs\n");
            List<JobStats> arr = jobPlan.getFailedJobs();
            for (JobStats js : arr) {   
                sb.append(js.getDisplayString());
            }
            sb.append("\n");
        }
        sb.append("Input(s):\n");
        for (InputStats is : getInputStats()) {
            sb.append(is.getDisplayString());
        }
        sb.append("\n");
        sb.append("Output(s):\n");
        for (OutputStats ds : getOutputStats()) {
            sb.append(ds.getDisplayString());
        }
        
        sb.append("\nCounters:\n");
        sb.append("Total records written : " + getRecordWritten()).append("\n");
        sb.append("Total bytes written : " + getBytesWritten()).append("\n");
        sb.append("Spillable Memory Manager spill count : "
                + getSMMSpillCount()).append("\n");
        sb.append("Total bags proactively spilled: " 
                + getProactiveSpillCountObjects()).append("\n");
        sb.append("Total records proactively spilled: " 
                + getProactiveSpillCountRecords()).append("\n");
        
        sb.append("\nJob DAG:\n").append(jobPlan.toString());
        
        LOG.info("Script Statistics: \n" + sb.toString());
    }
    
    @SuppressWarnings("deprecation")
    void mapMROperToJob(MapReduceOper mro, Job job) {
        if (mro == null) {
            LOG.warn("null MR operator");
        } else {
            JobStats js = mroJobMap.get(mro);
            if (js == null) {
                LOG.warn("null job stats for mro: " + mro.getOperatorKey());
            } else {
                JobID id = job.getAssignedJobID();
                js.setId(id);    
                if (id != null) {
                    jobMroMap.put(id.toString(), mro);
                } else {
                    jobMroMap.put(job.toString(), mro);
                }
            }
        }
    }

    void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
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
    
}
