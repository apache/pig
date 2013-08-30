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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;

/**
 * This class encapsulates the runtime statistics of a MapReduce job. 
 * Job statistics is collected when job is completed.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class JobStats extends Operator {
        
    public static final String ALIAS = "JobStatistics:alias";
    public static final String ALIAS_LOCATION = "JobStatistics:alias_location";
    public static final String FEATURE = "JobStatistics:feature";
        
    private static final Log LOG = LogFactory.getLog(JobStats.class);
    public static final String SUCCESS_HEADER = null;
    public static final String FAILURE_HEADER = null;
    
    public static enum JobState { UNKNOWN, SUCCESS, FAILED; }
    
    protected JobState state = JobState.UNKNOWN;
        
    protected ArrayList<OutputStats> outputs;
    
    protected ArrayList<InputStats> inputs;
       
    private String errorMsg;
    
    private Exception exception = null;
               
    protected JobStats(String name, JobGraph plan) {
        super(name, plan);
        outputs = new ArrayList<OutputStats>();
        inputs = new ArrayList<InputStats>();
    }

    public abstract String getJobId();
    
    public JobState getState() { return state; }
    
    public boolean isSuccessful() { return (state == JobState.SUCCESS); }

    public void setSuccessful(boolean isSuccessful) {
        this.state = isSuccessful ? JobState.SUCCESS : JobState.FAILED;
    }

    public String getErrorMessage() { return errorMsg; }
    
    public Exception getException() { return exception; }
    
    public List<OutputStats> getOutputs() {
        return Collections.unmodifiableList(outputs);
    }
    
    public List<InputStats> getInputs() {
        return Collections.unmodifiableList(inputs);
    }
       
    public String getAlias() {
        return (String)getAnnotation(ALIAS);
    }
    
    public String getAliasLocation() {
        return (String)getAnnotation(ALIAS_LOCATION);
    }

    public String getFeature() {
        return (String)getAnnotation(FEATURE);
    }
        
    /**
     * Returns the total bytes written to user specified HDFS
     * locations of this job.
     */
    public long getBytesWritten() {        
        long count = 0;
        for (OutputStats out : outputs) {
            long n = out.getBytes();            
            if (n > 0) count += n;
        }
        return count;
    }
    
    /**
     * Returns the total number of records in user specified output
     * locations of this job.
     */
    public long getRecordWrittern() {
        long count = 0;
        for (OutputStats out : outputs) {
            long rec = out.getNumberRecords();
            if (rec > 0) count += rec;
        }
        return count;
    }
    
    @Override
    public abstract void accept(PlanVisitor v) throws FrontendException;
    
    
    @Override
    public boolean isEqual(Operator operator) {
        if (!(operator instanceof JobStats)) return false;
        return name.equalsIgnoreCase(operator.getName());
    }    

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }
    
    public void setBackendException(Exception e) {
        exception = e;
    }
       
    public abstract String getDisplayString(boolean isLocal);

    
    /**
     * Calculate the median value from the given array
     * @param durations
     * @return median value
     */
    protected long calculateMedianValue(long[] durations) {
		long median;
		// figure out the median
		Arrays.sort(durations);
		int midPoint = durations.length /2;
		if ((durations.length & 1) == 1) {
			// odd
			median = durations[midPoint];
		} else {
			// even
			median = (durations[midPoint-1] + durations[midPoint]) / 2; 
		}
		return median;
	}
    
    public boolean isSampler() {
        return getFeature().contains(ScriptState.PIG_FEATURE.SAMPLER.name());
    }
    
    public boolean isIndexer() {
        return getFeature().contains(ScriptState.PIG_FEATURE.INDEXER.name());
    }
    
}
