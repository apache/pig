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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.local.executionengine.physicalLayer.counters.POCounter;
import org.apache.pig.impl.util.ObjectSerializer;

public class PigStats {
    MROperPlan mrp;
    PhysicalPlan php;
    JobControl jc;
    JobClient jobClient;
    Map<String, Map<String, String>> stats = new HashMap<String, Map<String,String>>();
    // String lastJobID;
    ArrayList<String> rootJobIDs = new ArrayList<String>();
    ExecType mode;
    
    public void setMROperatorPlan(MROperPlan mrp) {
        this.mrp = mrp;
    }
    
    public void setJobControl(JobControl jc) {
        this.jc = jc;
    }
    
    public void setJobClient(JobClient jobClient) {
        this.jobClient = jobClient;
    }
    
    public String getMRPlan() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mrp.dump(new PrintStream(baos));
        return baos.toString();
    }
    
    public void setExecType(ExecType mode) {
        this.mode = mode;
    }
    
    public void setPhysicalPlan(PhysicalPlan php) {
        this.php = php;
    }
    
    public String getPhysicalPlan() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        php.explain(baos);
        return baos.toString();
    }
    
    public Map<String, Map<String, String>> accumulateStats() throws ExecException {
        if(mode == ExecType.MAPREDUCE)
            return accumulateMRStats();
        else if(mode == ExecType.LOCAL)
            return accumulateLocalStats();
        else
            throw new RuntimeException("Unrecognized mode. Either MapReduce or Local mode expected.");
    }
    
    private Map<String, Map<String, String>> accumulateLocalStats() {
        //The counter placed before a store in the local plan should be able to get the number of records
        for(PhysicalOperator op : php.getLeaves()) {
            Map<String, String> jobStats = new HashMap<String, String>();
            stats.put(op.toString(), jobStats);
            POCounter counter = (POCounter) php.getPredecessors(op).get(0);
            jobStats.put("PIG_STATS_LOCAL_OUTPUT_RECORDS", (Long.valueOf(counter.getCount())).toString());
            jobStats.put("PIG_STATS_LOCAL_BYTES_WRITTEN", (Long.valueOf((new File(((POStore)op).getSFile().getFileName())).length())).toString());
        }
        return stats;
    }
    
    private Map<String, Map<String, String>> accumulateMRStats() throws ExecException {
        
        for(Job job : jc.getSuccessfulJobs()) {
            
            
            JobConf jobConf = job.getJobConf();
            
            
                RunningJob rj = null;
                try {
                    rj = jobClient.getJob(job.getAssignedJobID());
                } catch (IOException e1) {
                    String error = "Unable to get the job statistics from JobClient.";
                    throw new ExecException(error, e1);
                }
                if(rj == null)
                    continue;
                
                Map<String, String> jobStats = new HashMap<String, String>();
                stats.put(job.getAssignedJobID().toString(), jobStats);
                
                try {
                    PhysicalPlan plan = (PhysicalPlan) ObjectSerializer.deserialize(jobConf.get("pig.mapPlan"));
                    jobStats.put("PIG_STATS_MAP_PLAN", plan.toString());
                    plan = (PhysicalPlan) ObjectSerializer.deserialize(jobConf.get("pig.combinePlan"));
                    if(plan != null) {
                        jobStats.put("PIG_STATS_COMBINE_PLAN", plan.toString());
                    }
                    plan = (PhysicalPlan) ObjectSerializer.deserialize(jobConf.get("pig.reducePlan"));
                    if(plan != null) {
                        jobStats.put("PIG_STATS_REDUCE_PLAN", plan.toString());
                    }
                } catch (IOException e2) {
                    String error = "Error deserializing plans from the JobConf.";
                    throw new RuntimeException(error, e2);
                }
                
                Counters counters = null;
                try {
                    counters = rj.getCounters();
                    Counters.Group taskgroup = counters.getGroup("org.apache.hadoop.mapred.Task$Counter");
                    Counters.Group hdfsgroup = counters.getGroup("org.apache.hadoop.mapred.Task$FileSystemCounter");

                    jobStats.put("PIG_STATS_MAP_INPUT_RECORDS", (Long.valueOf(taskgroup.getCounterForName("MAP_INPUT_RECORDS").getCounter())).toString());
                    jobStats.put("PIG_STATS_MAP_OUTPUT_RECORDS", (Long.valueOf(taskgroup.getCounterForName("MAP_OUTPUT_RECORDS").getCounter())).toString());
                    jobStats.put("PIG_STATS_REDUCE_INPUT_RECORDS", (Long.valueOf(taskgroup.getCounterForName("REDUCE_INPUT_RECORDS").getCounter())).toString());
                    jobStats.put("PIG_STATS_REDUCE_OUTPUT_RECORDS", (Long.valueOf(taskgroup.getCounterForName("REDUCE_OUTPUT_RECORDS").getCounter())).toString());
                    jobStats.put("PIG_STATS_BYTES_WRITTEN", (Long.valueOf(hdfsgroup.getCounterForName("HDFS_WRITE").getCounter())).toString());
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    String error = "Unable to get the counters.";
                    throw new ExecException(error, e);
                }
                
            
            
        }
        
        getLastJobIDs(jc.getSuccessfulJobs());
        
        return stats;
    }
    

    private void getLastJobIDs(List<Job> jobs) {
        rootJobIDs.clear();
         Set<Job> temp = new HashSet<Job>();
         for(Job job : jobs) {
             if(job.getDependingJobs() != null && job.getDependingJobs().size() > 0)
                 temp.addAll(job.getDependingJobs());
         }
         
         //difference between temp and jobs would be the set of leaves
         //we can safely assume there would be only one leaf
         for(Job job : jobs) {
             if(temp.contains(job)) continue;
             else rootJobIDs.add(job.getAssignedJobID().toString());
         }
    }
    
    public List<String> getRootJobIDs() {
        return rootJobIDs;
    }
    
    public Map<String, Map<String, String>> getPigStats() {
        return stats;
    }
    
    public long getRecordsWritten() {
        if(mode == ExecType.LOCAL)
            return getRecordsCountLocal();
        else if(mode == ExecType.MAPREDUCE)
            return getRecordsCountMR();
        else
            throw new RuntimeException("Unrecognized mode. Either MapReduce or Local mode expected.");
    }
    
    private long getRecordsCountLocal() {
        //System.out.println(getPhysicalPlan());
        //because of the nature of the parser, there will always be only one store

        for(PhysicalOperator op : php.getLeaves()) {
            return Long.parseLong(stats.get(op.toString()).get("PIG_STATS_LOCAL_OUTPUT_RECORDS"));
        }
        return 0;
    }
    
    /**
     * Returns the no. of records written by the pig script in MR mode
     * @return
     */
    private long getRecordsCountMR() {
        long records = 0;
        for (String jid : rootJobIDs) {
            Map<String, String> jobStats = stats.get(jid);
            if (jobStats == null) continue;
            String reducePlan = jobStats.get("PIG_STATS_REDUCE_PLAN");
        	if(reducePlan == null) {
            	records += Long.parseLong(jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS"));
        	} else {
            	records += Long.parseLong(jobStats.get("PIG_STATS_REDUCE_OUTPUT_RECORDS"));
        	}
        }
    	return records;
    }
    
    public long getBytesWritten() {
    	if(mode == ExecType.LOCAL) {
    		return getLocalBytesWritten();
    	} else if(mode == ExecType.MAPREDUCE) {
    		return getMapReduceBytesWritten();
    	} else {
    		throw new RuntimeException("Unrecognized mode. Either MapReduce or Local mode expected.");
    	}
    	
    }
    
    private long getLocalBytesWritten() {
    	for(PhysicalOperator op : php.getLeaves())
    		return Long.parseLong(stats.get(op.toString()).get("PIG_STATS_LOCAL_BYTES_WRITTEN"));
    	return 0;
    }
    
    private long getMapReduceBytesWritten() {
        long bytesWritten = 0;
        for (String jid : rootJobIDs) {
            Map<String, String> jobStats = stats.get(jid);
            if (jobStats == null) continue;
            bytesWritten += Long.parseLong(jobStats.get("PIG_STATS_BYTES_WRITTEN"));
        }
        return bytesWritten;
    }
    
}
