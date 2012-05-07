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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.pig.PigException;
import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.SpillableMemoryManager;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.tools.pigstats.JobStats.JobState;
import org.apache.pig.tools.pigstats.SimplePigStats.JobGraphPrinter;

/**
 * PigStats encapsulates the statistics collected from a running script. 
 * It includes status of the execution, the DAG of its MR jobs, as well as 
 * information about outputs and inputs of the script. 
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class PigStats {
    
    private static final Log LOG = LogFactory.getLog(PigStats.class);
    
    private static ThreadLocal<PigStats> tps = new ThreadLocal<PigStats>();
    
    protected int returnCode = ReturnCode.UNKNOWN;
    
    private String errorMessage;
    private int errorCode = -1;
    
    public static PigStats get() {
        if (tps.get() == null) tps.set(new SimplePigStats());
        return tps.get();
    }
    
    static void set(PigStats stats) {
        tps.set(stats);
    }
        
    static PigStats start() {
        tps.set(new SimplePigStats());
        return tps.get();
    }
    
    /**
     * Returns code are defined in {@link ReturnCode}
     */
    public int getReturnCode() {
        return returnCode;
    }
    
    /**
     * Returns error message string
     */
    public String getErrorMessage() {
        return errorMessage;
    }
    
    /**
     * Returns the error code of {@link PigException}
     */
    public int getErrorCode() {
        return errorCode;
    }

    public abstract JobClient getJobClient();

    public abstract boolean isEmbedded();
    
    public abstract boolean isSuccessful();
 
    public abstract Map<String, List<PigStats>> getAllStats();
    
    public abstract List<String> getAllErrorMessages();       
        
    /**
     * Returns the properties associated with the script
     */
    public abstract Properties getPigProperties();
    
    /**
     * Returns the DAG of the MR jobs spawned by the script
     */
    public abstract JobGraph getJobGraph();
    
    /**
     * Returns the list of output locations in the script
     */
    public abstract List<String> getOutputLocations();
    
    /**
     * Returns the list of output names in the script
     */
    public abstract List<String> getOutputNames();
    
    /**
     * Returns the number of bytes for the given output location,
     * -1 for invalid location or name.
     */
    public abstract long getNumberBytes(String location);
    
    /**
     * Returns the number of records for the given output location,
     * -1 for invalid location or name.
     */
    public abstract long getNumberRecords(String location);
        
    /**
     * Returns the alias associated with this output location
     */
    public abstract String getOutputAlias(String location);
    
    /**
     * Returns the total spill counts from {@link SpillableMemoryManager}.
     */
    public abstract long getSMMSpillCount();
    
    /**
     * Returns the total number of bags that spilled proactively
     */
    public abstract long getProactiveSpillCountObjects();
    
    /**
     * Returns the total number of records that spilled proactively
     */
    public abstract long getProactiveSpillCountRecords();
    
    /**
     * Returns the total bytes written to user specified HDFS
     * locations of this script.
     */
    public abstract long getBytesWritten();
    
    /**
     * Returns the total number of records in user specified output
     * locations of this script.
     */
    public abstract long getRecordWritten();

    public String getHadoopVersion() {
        return ScriptState.get().getHadoopVersion();
    }
    
    public String getPigVersion() {
        return ScriptState.get().getPigVersion();
    }
   
    public abstract String getScriptId();
    
    public abstract String getFeatures();
    
    public abstract long getDuration();
    
    /**
     * Returns the number of MR jobs for this script
     */
    public abstract int getNumberJobs();
        
    public abstract List<OutputStats> getOutputStats();
    
    public abstract OutputStats result(String alias);
    
    public abstract List<InputStats> getInputStats();    
    
    void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    } 
    /**
     * JobGraph is an {@link OperatorPlan} whose members are {@link JobStats}
     */
    public static class JobGraph extends BaseOperatorPlan implements Iterable<JobStats>{
                
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
 
        public boolean isConnected(Operator from, Operator to) {
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
    
    private static class JobComparator implements Comparator<JobStats> {
        @Override
        public int compare(JobStats o1, JobStats o2) {           
            return o1.getJobId().compareTo(o2.getJobId());
        }       
    }    
    
    void setReturnCode(int returnCode) {
        this.returnCode = returnCode; 
    }
}
