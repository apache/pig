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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.pig.PigCounters;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.SimplePigStats.JobGraphPrinter;

/**
 * This class encapsulates the runtime statistics of a MapReduce job. 
 * Job statistics is collected when job is completed.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class JobStats extends Operator {
        
    public static final String ALIAS = "JobStatistics:alias";
    public static final String ALIAS_LOCATION = "JobStatistics:alias_location";
    public static final String FEATURE = "JobStatistics:feature";
    
    public static final String SUCCESS_HEADER = "JobId\tMaps\tReduces\t" +
    		"MaxMapTime\tMinMapTIme\tAvgMapTime\tMedianMapTime\tMaxReduceTime\t" +
    		"MinReduceTime\tAvgReduceTime\tMedianReducetime\tAlias\tFeature\tOutputs";
   
    public static final String FAILURE_HEADER = "JobId\tAlias\tFeature\tMessage\tOutputs";
    
    // currently counters are not working in local mode - see PIG-1286
    public static final String SUCCESS_HEADER_LOCAL = "JobId\tAlias\tFeature\tOutputs";
    
    private static final Log LOG = LogFactory.getLog(JobStats.class);
    
    public static enum JobState { UNKNOWN, SUCCESS, FAILED; }
    
    private JobState state = JobState.UNKNOWN;
        
    private Configuration conf;
    
    private List<POStore> mapStores = null;
    
    private List<POStore> reduceStores = null;
    
    private List<FileSpec> loads = null;
    
    private ArrayList<OutputStats> outputs;
    
    private ArrayList<InputStats> inputs;
       
    private String errorMsg;
    
    private Exception exception = null;
    
    private Boolean disableCounter = false;
            
    @SuppressWarnings("deprecation")
    private JobID jobId;
    
    private long maxMapTime = 0;
    private long minMapTime = 0;
    private long avgMapTime = 0;
    private long medianMapTime = 0;
    private long maxReduceTime = 0;
    private long minReduceTime = 0;
    private long avgReduceTime = 0;
    private long medianReduceTime = 0;

    private int numberMaps = 0;
    private int numberReduces = 0;
    
    private long mapInputRecords = 0;
    private long mapOutputRecords = 0;
    private long reduceInputRecords = 0;
    private long reduceOutputRecords = 0;
    private long hdfsBytesWritten = 0;
    private long hdfsBytesRead = 0;
    private long spillCount = 0;
    private long activeSpillCountObj = 0;
    private long activeSpillCountRecs = 0;
    
    private HashMap<String, Long> multiStoreCounters 
            = new HashMap<String, Long>();
    
    private HashMap<String, Long> multiInputCounters 
            = new HashMap<String, Long>();
        
    @SuppressWarnings("deprecation")
    private Counters counters = null;
    
    JobStats(String name, JobGraph plan) {
        super(name, plan);
        outputs = new ArrayList<OutputStats>();
        inputs = new ArrayList<InputStats>();
    }

    public String getJobId() { 
        return (jobId == null) ? null : jobId.toString(); 
    }
    
    public JobState getState() { return state; }
    
    public boolean isSuccessful() { return (state == JobState.SUCCESS); }
    
    public String getErrorMessage() { return errorMsg; }
    
    public Exception getException() { return exception; }
    
    public int getNumberMaps() { return numberMaps; }
    
    public int getNumberReduces() { return numberReduces; }
    
    public long getMaxMapTime() { return maxMapTime; }
    
    public long getMinMapTime() { return minMapTime; }
    
    public long getAvgMapTime() { return avgMapTime; }
    
    public long getMaxReduceTime() { return maxReduceTime; }
    
    public long getMinReduceTime() { return minReduceTime; }
    
    public long getAvgREduceTime() { return avgReduceTime; }           
        
    public long getMapInputRecords() { return mapInputRecords; }

    public long getMapOutputRecords() { return mapOutputRecords; }

    public long getReduceOutputRecords() { return reduceOutputRecords; }

    public long getReduceInputRecords() { return reduceInputRecords; }

    public long getSMMSpillCount() { return spillCount; }
    
    public long getProactiveSpillCountObjects() { return activeSpillCountObj; }
    
    public long getProactiveSpillCountRecs() { return activeSpillCountRecs; }
    
    public long getHdfsBytesWritten() { return hdfsBytesWritten; }
    
    @SuppressWarnings("deprecation")
    public Counters getHadoopCounters() { return counters; }
    
    public List<OutputStats> getOutputs() {
        return Collections.unmodifiableList(outputs);
    }
    
    public List<InputStats> getInputs() {
        return Collections.unmodifiableList(inputs);
    }

    public Map<String, Long> getMultiStoreCounters() {
        return Collections.unmodifiableMap(multiStoreCounters);
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
    public void accept(PlanVisitor v) throws FrontendException {
        if (v instanceof JobGraphPrinter) {
            JobGraphPrinter jpp = (JobGraphPrinter)v;
            jpp.visit(this);
        }
    }

    @Override
    public boolean isEqual(Operator operator) {
        if (!(operator instanceof JobStats)) return false;
        return name.equalsIgnoreCase(operator.getName());
    }    
 

    @SuppressWarnings("deprecation")
    void setId(JobID jobId) {
        this.jobId = jobId;
    }
    
    void setSuccessful(boolean isSuccessful) {
        this.state = isSuccessful ? JobState.SUCCESS : JobState.FAILED;
    }
    
    void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }
    
    void setBackendException(Exception e) {
        exception = e;
    }
        
    @SuppressWarnings("unchecked")
    void setConf(Configuration conf) {        
        if (conf == null) return;
        this.conf = conf;
        try {
            this.mapStores = (List<POStore>) ObjectSerializer.deserialize(conf
                    .get(JobControlCompiler.PIG_MAP_STORES));
            this.reduceStores = (List<POStore>) ObjectSerializer.deserialize(conf
                    .get(JobControlCompiler.PIG_REDUCE_STORES));           
            this.loads = (ArrayList<FileSpec>) ObjectSerializer.deserialize(conf
                    .get("pig.inputs"));
            this.disableCounter = conf.getBoolean("pig.disable.counter", false);
        } catch (IOException e) {
            LOG.warn("Failed to deserialize the store list", e);
        }                    
    }
    
    void setMapStat(int size, long max, long min, long avg, long median) {
        numberMaps = size;
        maxMapTime = max;
        minMapTime = min;
        avgMapTime = avg;    
        medianMapTime = median;
    }
    
    void setReduceStat(int size, long max, long min, long avg, long median) {
        numberReduces = size;
        maxReduceTime = max;
        minReduceTime = min;
        avgReduceTime = avg;       
        medianReduceTime = median;
    }  
    
    String getDisplayString(boolean local) {
        StringBuilder sb = new StringBuilder();
        String id = (jobId == null) ? "N/A" : jobId.toString();
        if (state == JobState.FAILED || local) {           
            sb.append(id).append("\t")
                .append(getAlias()).append("\t")
                .append(getFeature()).append("\t");
            if (state == JobState.FAILED) {
                sb.append("Message: ").append(getErrorMessage()).append("\t");
            }
        } else if (state == JobState.SUCCESS) {
            sb.append(id).append("\t")
                .append(numberMaps).append("\t")
                .append(numberReduces).append("\t");
            if (maxMapTime < 0) {
                sb.append("n/a\t").append("n/a\t").append("n/a\t");
            } else { 
                sb.append(maxMapTime/1000).append("\t")
                    .append(minMapTime/1000).append("\t")
                    .append(avgMapTime/1000).append("\t")
                    .append(medianMapTime/1000).append("\t");
            }
            if (maxReduceTime < 0) {
                sb.append("n/a\t").append("n/a\t").append("n/a\t");
            } else {
                sb.append(maxReduceTime/1000).append("\t")
                    .append(minReduceTime/1000).append("\t")
                    .append(avgReduceTime/1000).append("\t")
                    .append(medianReduceTime/1000).append("\t");
            }
            sb.append(getAlias()).append("\t")
                .append(getFeature()).append("\t");
        }
        for (OutputStats os : outputs) {
            sb.append(os.getLocation()).append(",");
        }        
        sb.append("\n");
        return sb.toString();
    }

    @SuppressWarnings("deprecation")
    void addCounters(RunningJob rjob) {
        if (rjob != null) {
            try {
                counters = rjob.getCounters();
            } catch (IOException e) {
                LOG.warn("Unable to get job counters", e);
            }
        }
        if (counters != null) {
            Counters.Group taskgroup = counters
                    .getGroup(PigStatsUtil.TASK_COUNTER_GROUP);
            Counters.Group hdfsgroup = counters
                    .getGroup(PigStatsUtil.FS_COUNTER_GROUP);
            Counters.Group multistoregroup = counters
                    .getGroup(PigStatsUtil.MULTI_STORE_COUNTER_GROUP);
            Counters.Group multiloadgroup = counters
                    .getGroup(PigStatsUtil.MULTI_INPUTS_COUNTER_GROUP);

            mapInputRecords = taskgroup.getCounterForName(
                    PigStatsUtil.MAP_INPUT_RECORDS).getCounter();
            mapOutputRecords = taskgroup.getCounterForName(
                    PigStatsUtil.MAP_OUTPUT_RECORDS).getCounter();
            reduceInputRecords = taskgroup.getCounterForName(
                    PigStatsUtil.REDUCE_INPUT_RECORDS).getCounter();
            reduceOutputRecords = taskgroup.getCounterForName(
                    PigStatsUtil.REDUCE_OUTPUT_RECORDS).getCounter();
            hdfsBytesRead = hdfsgroup.getCounterForName(
                    PigStatsUtil.HDFS_BYTES_READ).getCounter();      
            hdfsBytesWritten = hdfsgroup.getCounterForName(
                    PigStatsUtil.HDFS_BYTES_WRITTEN).getCounter();            
            spillCount = counters.findCounter(
                    PigCounters.SPILLABLE_MEMORY_MANAGER_SPILL_COUNT)
                    .getCounter();
            activeSpillCountObj = counters.findCounter(
                    PigCounters.PROACTIVE_SPILL_COUNT_BAGS).getCounter();
            activeSpillCountRecs = counters.findCounter(
                    PigCounters.PROACTIVE_SPILL_COUNT_RECS).getCounter();

            Iterator<Counter> iter = multistoregroup.iterator();
            while (iter.hasNext()) {
                Counter cter = iter.next();
                multiStoreCounters.put(cter.getName(), cter.getValue());
            }     
            
            Iterator<Counter> iter2 = multiloadgroup.iterator();
            while (iter2.hasNext()) {
                Counter cter = iter2.next();
                multiInputCounters.put(cter.getName(), cter.getValue());
            } 
            
        }              
    }
    
    void addMapReduceStatistics(JobClient client, Configuration conf) {
        TaskReport[] maps = null;
        try {
            maps = client.getMapTaskReports(jobId);
        } catch (IOException e) {
            LOG.warn("Failed to get map task report", e);            
        }
        if (maps != null && maps.length > 0) {
            int size = maps.length;
            long max = 0;
            long min = Long.MAX_VALUE;
            long median = 0;
            long total = 0;
            long durations[] = new long[size];
            
            for (int i = 0; i < maps.length; i++) {
            	TaskReport rpt = maps[i];
                long duration = rpt.getFinishTime() - rpt.getStartTime();
                durations[i] = duration;
                max = (duration > max) ? duration : max;
                min = (duration < min) ? duration : min;
                total += duration;
            }
            long avg = total / size;
            
            median = calculateMedianValue(durations);
            setMapStat(size, max, min, avg, median);
        } else {
            int m = conf.getInt("mapred.map.tasks", 1);
            if (m > 0) {
                setMapStat(m, -1, -1, -1, -1);
            }
        }
        
        TaskReport[] reduces = null;
        try {
            reduces = client.getReduceTaskReports(jobId);
        } catch (IOException e) {
            LOG.warn("Failed to get reduce task report", e);
        }
        if (reduces != null && reduces.length > 0) {
            int size = reduces.length;
            long max = 0;
            long min = Long.MAX_VALUE;
            long median = 0;
            long total = 0;
            long durations[] = new long[size];
            
            for (int i = 0; i < reduces.length; i++) {
            	TaskReport rpt = reduces[i];
                long duration = rpt.getFinishTime() - rpt.getStartTime();
                durations[i] = duration;
                max = (duration > max) ? duration : max;
                min = (duration < min) ? duration : min;
                total += duration;
            }
            long avg = total / size;
            median = calculateMedianValue(durations);
            setReduceStat(size, max, min, avg, median);
        } else {
            int m = conf.getInt("mapred.reduce.tasks", 1);
            if (m > 0) {
                setReduceStat(m, -1, -1, -1, -1);
            }
        }
    }

    /**
     * Calculate the median value from the given array
     * @param durations
     * @return median value
     */
	private long calculateMedianValue(long[] durations) {
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
    
    void setAlias(MapReduceOper mro) {       
        annotate(ALIAS, ScriptState.get().getAlias(mro));             
        annotate(ALIAS_LOCATION, ScriptState.get().getAliasLocation(mro));
        annotate(FEATURE, ScriptState.get().getPigFeature(mro));
    }
    
    void addOutputStatistics() {
        if (mapStores == null || reduceStores == null) {
            LOG.warn("unable to get stores of the job");
            return;
        }
        
        if (mapStores.size() + reduceStores.size() == 1) {
            POStore sto = (mapStores.size() > 0) ? mapStores.get(0)
                    : reduceStores.get(0);
            if (!sto.isTmpStore()) {
                long records = (mapStores.size() > 0) ? mapOutputRecords
                        : reduceOutputRecords;           
                OutputStats ds = new OutputStats(sto.getSFile().getFileName(),
                        hdfsBytesWritten, records, (state == JobState.SUCCESS));
                ds.setPOStore(sto);
                ds.setConf(conf);
                outputs.add(ds);
                
                if (state == JobState.SUCCESS) {
                    ScriptState.get().emitOutputCompletedNotification(ds);
                }
            }
        } else {
            for (POStore sto : mapStores) {
                if (sto.isTmpStore()) continue;
                addOneOutputStats(sto);
            }
            for (POStore sto : reduceStores) {
                if (sto.isTmpStore()) continue;
                addOneOutputStats(sto);
            }     
        }
    }
    
    private void addOneOutputStats(POStore sto) {
        long records = -1;
        if (sto.isMultiStore()) {
            Long n = multiStoreCounters.get(PigStatsUtil.getMultiStoreCounterName(sto));
            if (n != null) records = n;
        } else {
            records = mapOutputRecords;
        }
        String location = sto.getSFile().getFileName();        
        URI uri = null;
        try {
            uri = new URI(location);
        } catch (URISyntaxException e1) {
            LOG.warn("invalid syntax for output location: " + location, e1);
        }
        long bytes = -1;
        if (uri != null
                && (uri.getScheme() == null || uri.getScheme()
                        .equalsIgnoreCase("hdfs"))) {
            try {
                Path p = new Path(location);
                FileSystem fs = p.getFileSystem(conf);
                FileStatus[] lst = fs.listStatus(p);
                if (lst != null) {
                    for (FileStatus status : lst) {
                        bytes += status.getLen();
                    } 
                }
            } catch (IOException e) {
                LOG.warn("unable to get byte written of the job", e);
            }
        }
        OutputStats ds = new OutputStats(location, bytes, records,
                (state == JobState.SUCCESS));  
        ds.setPOStore(sto);
        ds.setConf(conf);
        outputs.add(ds);
        
        if (state == JobState.SUCCESS) {
            ScriptState.get().emitOutputCompletedNotification(ds);
        }
    }
       
    void addInputStatistics() {
        if (loads == null)  {
            LOG.warn("unable to get inputs of the job");
            return;
        }
        
        if (loads.size() == 1) {
            FileSpec fsp = loads.get(0); 
            if (!PigStatsUtil.isTempFile(fsp.getFileName())) {
                long records = mapInputRecords;       
                InputStats is = new InputStats(fsp.getFileName(),
                        hdfsBytesRead, records, (state == JobState.SUCCESS));              
                is.setConf(conf);
                if (isSampler()) is.markSampleInput();
                if (isIndexer()) is.markIndexerInput();
                inputs.add(is);                
            }
        } else {
            for (int i=0; i<loads.size(); i++) {
                FileSpec fsp = loads.get(i);
                if (PigStatsUtil.isTempFile(fsp.getFileName())) continue;
                addOneInputStats(fsp.getFileName(), i);
            }
        }            
    }
    
    private void addOneInputStats(String fileName, int index) {
        long records = -1;
        Long n = multiInputCounters.get(
                PigStatsUtil.getMultiInputsCounterName(fileName, index));
        if (n != null) {   
            records = n;
        } else {
            // the file could be empty
            if (!disableCounter) records = 0;
            else {
                LOG.warn("unable to get input counter for " + fileName);
            }
        }
        InputStats is = new InputStats(fileName, -1, records, (state == JobState.SUCCESS));              
        is.setConf(conf);
        inputs.add(is);
    }
    
    private boolean isSampler() {
        return getFeature().contains(ScriptState.PIG_FEATURE.SAMPLER.name());
    }
    
    private boolean isIndexer() {
        return getFeature().contains(ScriptState.PIG_FEATURE.INDEXER.name());
    }
    
}
