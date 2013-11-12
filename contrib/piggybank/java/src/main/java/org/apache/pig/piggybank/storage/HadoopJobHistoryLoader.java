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
package org.apache.pig.piggybank.storage;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;

import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.DefaultJobHistoryParser;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class HadoopJobHistoryLoader extends LoadFunc {
         
    private static final Log LOG = LogFactory.getLog(HadoopJobHistoryLoader.class);
       
    private RecordReader<Text, MRJobInfo> reader;
    
    public HadoopJobHistoryLoader() {
    }
   
    @SuppressWarnings("unchecked")
    @Override
    public InputFormat getInputFormat() throws IOException {     
        return new HadoopJobHistoryInputFormat();
    }

    @Override
    public Tuple getNext() throws IOException {
        boolean notDone = false;
        try {
            notDone = reader.nextKeyValue();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        if (!notDone) {
            return null;
        }   
        Tuple t = null;
        try {
            MRJobInfo val = (MRJobInfo)reader.getCurrentValue();
            t = DefaultTupleFactory.getInstance().newTuple(3);
            t.set(0, val.job);
            t.set(1, val.mapTask);
            t.set(2, val.reduceTask);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return t;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        this.reader = (HadoopJobHistoryReader)reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
        FileInputFormat.setInputPathFilter(job, JobHistoryPathFilter.class);
    }
    
    public static class JobHistoryPathFilter implements PathFilter {
        @Override
        public boolean accept(Path p) {
            String name = p.getName(); 
            return !name.endsWith(".xml");
        }       
    }
    
    public static class HadoopJobHistoryInputFormat extends
            FileInputFormat<Text, MRJobInfo> {

        @Override
        public RecordReader<Text, MRJobInfo> createRecordReader(
                InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new HadoopJobHistoryReader();
        }

        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }         
    }
    
    public static class HadoopJobHistoryReader extends
            RecordReader<Text, MRJobInfo> {

        private String location;
        
        private MRJobInfo value;
        
        private Configuration conf;
                
        @Override
        public void close() throws IOException {            
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return null;
        }

        @Override
        public MRJobInfo getCurrentValue() throws IOException,
                InterruptedException {            
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            FileSplit fSplit = (FileSplit) split; 
            Path p = fSplit.getPath();
            location = p.toString();
            LOG.info("location: " + location);    
            conf = context.getConfiguration();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (location != null) {
                LOG.info("load: " + location);  
                Path full = new Path(location);  
                String[] jobDetails = 
                    JobInfo.decodeJobHistoryFileName(full.getName()).split("_");
                String jobId = jobDetails[2] + "_" + jobDetails[3] + "_"
                        + jobDetails[4];
                JobHistory.JobInfo job = new JobHistory.JobInfo(jobId); 
 
                value = new MRJobInfo();
                                            
                FileSystem fs = full.getFileSystem(conf);
                FileStatus fstat = fs.getFileStatus(full);
                
                LOG.info("file size: " + fstat.getLen());
                DefaultJobHistoryParser.parseJobTasks(location, job,
                        full.getFileSystem(conf)); 
                LOG.info("job history parsed sucessfully");
                HadoopJobHistoryLoader.parseJobHistory(conf, job, value);
                LOG.info("get parsed job history");
                
                // parse Hadoop job xml file
                Path parent = full.getParent();
                String jobXml = jobDetails[0] + "_" + jobDetails[1] + "_" + jobDetails[2] + "_conf.xml";
                Path p = new Path(parent, jobXml);  
             
                FSDataInputStream fileIn = fs.open(p);
                Map<String, String> val = HadoopJobHistoryLoader
                        .parseJobXML(fileIn);
                for (String key : val.keySet()) {
                    value.job.put(key, val.get(key));
                }
                
                location = null;
                return true;
            }          
            value = null;
            return false;
        }   
    }
    
    //------------------------------------------------------------------------
        
    public static class MRJobInfo {
        public Map<String, String> job;
        public Map<String, String> mapTask;
        public Map<String, String> reduceTask;
        
        public MRJobInfo() {
            job = new HashMap<String, String>();
            mapTask = new HashMap<String, String>();
            reduceTask = new HashMap<String, String>();
        }
    }
    
    //--------------------------------------------------------------------------------------------
    
    public static final String TASK_COUNTER_GROUP = "org.apache.hadoop.mapred.Task$Counter";
    public static final String MAP_INPUT_RECORDS = "MAP_INPUT_RECORDS";
    public static final String REDUCE_INPUT_RECORDS = "REDUCE_INPUT_RECORDS";
    
    /**
     * Job Keys
     */
    public static enum JobKeys {
        JOBTRACKERID, JOBID, JOBNAME, JOBTYPE, USER, SUBMIT_TIME, CONF_PATH, LAUNCH_TIME, TOTAL_MAPS, TOTAL_REDUCES,
        STATUS, FINISH_TIME, FINISHED_MAPS, FINISHED_REDUCES, FAILED_MAPS, FAILED_REDUCES, 
        LAUNCHED_MAPS, LAUNCHED_REDUCES, RACKLOCAL_MAPS, DATALOCAL_MAPS, HDFS_BYTES_READ,
        HDFS_BYTES_WRITTEN, FILE_BYTES_READ, FILE_BYTES_WRITTEN, COMBINE_OUTPUT_RECORDS,
        COMBINE_INPUT_RECORDS, REDUCE_INPUT_GROUPS, REDUCE_INPUT_RECORDS, REDUCE_OUTPUT_RECORDS,
        MAP_INPUT_RECORDS, MAP_OUTPUT_RECORDS, MAP_INPUT_BYTES, MAP_OUTPUT_BYTES, MAP_HDFS_BYTES_WRITTEN,
        JOBCONF, JOB_PRIORITY, SHUFFLE_BYTES, SPILLED_RECORDS
    }
    
    public static void parseJobHistory(Configuration jobConf, JobInfo jobInfo, MRJobInfo value) {
        value.job.clear();
        populateJob(jobInfo.getValues(), value.job);
        value.mapTask.clear();
        value.reduceTask.clear();
        populateMapReduceTaskLists(value, jobInfo.getAllTasks());
    }
    
    private static void populateJob (Map<JobHistory.Keys, String> jobC, Map<String, String> job) {            
        int size = jobC.size();
        Iterator<Map.Entry<JobHistory.Keys, String>> kv = jobC.entrySet().iterator();
        for (int i = 0; i < size; i++) {
            Map.Entry<JobHistory.Keys, String> entry = (Map.Entry<JobHistory.Keys, String>) kv.next();
            JobHistory.Keys key = entry.getKey();
            String value = entry.getValue();
            switch (key) {
            case JOBTRACKERID: job.put(JobKeys.JOBTRACKERID.toString(), value); break;           
            case FINISH_TIME: job.put(JobKeys.FINISH_TIME.toString(), value); break;
            case JOBID: job.put(JobKeys.JOBID.toString(), value); break;
            case JOBNAME: job.put(JobKeys.JOBNAME.toString(), value); break;
            case USER: job.put(JobKeys.USER.toString(), value); break;
            case JOBCONF: job.put(JobKeys.JOBCONF.toString(), value); break;
            case SUBMIT_TIME: job.put(JobKeys.SUBMIT_TIME.toString(), value); break;
            case LAUNCH_TIME: job.put(JobKeys.LAUNCH_TIME.toString(), value); break;
            case TOTAL_MAPS: job.put(JobKeys.TOTAL_MAPS.toString(), value); break;
            case TOTAL_REDUCES: job.put(JobKeys.TOTAL_REDUCES.toString(), value); break;
            case FAILED_MAPS: job.put(JobKeys.FAILED_MAPS.toString(), value); break;
            case FAILED_REDUCES: job.put(JobKeys.FAILED_REDUCES.toString(), value); break;
            case FINISHED_MAPS: job.put(JobKeys.FINISHED_MAPS.toString(), value); break;
            case FINISHED_REDUCES: job.put(JobKeys.FINISHED_REDUCES.toString(), value); break;
            case JOB_STATUS: job.put(JobKeys.STATUS.toString(), value); break;
            case COUNTERS:
                value.concat(",");
                parseAndAddJobCounters(job, value);
                break;
            default: 
                LOG.debug("JobHistory.Keys."+ key + " : NOT INCLUDED IN LOADER RETURN VALUE");
                break;
            }
        }
    }
    
    /*
     * Parse and add the job counters
     */
    @SuppressWarnings("deprecation")
    private static void parseAndAddJobCounters(Map<String, String> job, String counters) {
        try {
            Counters counterGroups = Counters.fromEscapedCompactString(counters);
            for (Group otherGroup : counterGroups) {
                Group group = counterGroups.getGroup(otherGroup.getName());
                for (Counter otherCounter : otherGroup) {
                    Counter counter = group.getCounterForName(otherCounter.getName());
                    job.put(otherCounter.getName(), String.valueOf(counter.getValue()));
                }
            }
        } catch (ParseException e) {
           LOG.warn("Failed to parse job counters", e);
        }
    } 
    
    @SuppressWarnings("deprecation")
    private static void populateMapReduceTaskLists (MRJobInfo value, 
            Map<String, JobHistory.Task> taskMap) {
                
        Map<String, String> mapT = value.mapTask;
        Map<String, String> reduceT = value.reduceTask;
        long minMapRows = Long.MAX_VALUE;
        long maxMapRows = 0;
        long minMapTime = Long.MAX_VALUE;
        long maxMapTime = 0;
        long avgMapTime = 0;
        long totalMapTime = 0;
        int numberMaps = 0;
        
        long minReduceRows = Long.MAX_VALUE;
        long maxReduceRows = 0;        
        long minReduceTime = Long.MAX_VALUE;
        long maxReduceTime = 0;
        long avgReduceTime = 0;
        long totalReduceTime = 0;
        int numberReduces = 0;
       
        int num_tasks = taskMap.entrySet().size();
        Iterator<Map.Entry<String, JobHistory.Task>> ti = taskMap.entrySet().iterator();
        for (int i = 0; i < num_tasks; i++) {
            Map.Entry<String, JobHistory.Task> entry = (Map.Entry<String, JobHistory.Task>) ti.next();
            JobHistory.Task task = entry.getValue();
            if (task.get(Keys.TASK_TYPE).equals("MAP")) {
                Map<JobHistory.Keys, String> mapTask = task.getValues();
                Map<JobHistory.Keys, String> successTaskAttemptMap  =  getLastSuccessfulTaskAttempt(task);
                // NOTE: Following would lead to less number of actual tasks collected in the tasklist array
                if (successTaskAttemptMap != null) {
                    mapTask.putAll(successTaskAttemptMap);
                } else {
                    LOG.info("Task:<" + task.get(Keys.TASKID) + "> is not successful - SKIPPING");
                }
                long duration = 0;
                long startTime = 0;
                long endTime = 0;
                int size = mapTask.size();
                numberMaps++;
                Iterator<Map.Entry<JobHistory.Keys, String>> kv = mapTask.entrySet().iterator();
                for (int j = 0; j < size; j++) {
                    Map.Entry<JobHistory.Keys, String> mtc = kv.next();
                    JobHistory.Keys key = mtc.getKey();
                    String val = mtc.getValue();
                    switch (key) {
                    case START_TIME: 
                        startTime = Long.valueOf(val);
                        break;
                    case FINISH_TIME:
                        endTime = Long.valueOf(val);
                        break;                    
                    case COUNTERS: {
                        try {
                            Counters counters = Counters.fromEscapedCompactString(val);
                            long rows = counters.getGroup(TASK_COUNTER_GROUP)
                                    .getCounterForName(MAP_INPUT_RECORDS).getCounter(); 
                            if (rows < minMapRows) minMapRows = rows;
                            if (rows > maxMapRows) maxMapRows = rows;
                        } catch (ParseException e) {
                            LOG.warn("Failed to parse job counters", e);
                        }
                    }
                    break;
                    default: 
                        LOG.warn("JobHistory.Keys." + key 
                                + " : NOT INCLUDED IN PERFORMANCE ADVISOR MAP COUNTERS");
                        break;
                    }
                }
                duration = endTime - startTime;
                if (minMapTime > duration) minMapTime = duration;
                if (maxMapTime < duration) maxMapTime = duration;
                totalMapTime += duration;        
            } else if (task.get(Keys.TASK_TYPE).equals("REDUCE")) {
                Map<JobHistory.Keys, String> reduceTask = task.getValues();
                Map<JobHistory.Keys, String> successTaskAttemptMap  =  getLastSuccessfulTaskAttempt(task);
                // NOTE: Following would lead to less number of actual tasks collected in the tasklist array
                if (successTaskAttemptMap != null) {
                    reduceTask.putAll(successTaskAttemptMap);
                } else {
                    LOG.warn("Task:<" + task.get(Keys.TASKID) + "> is not successful - SKIPPING");
                }
                long duration = 0;
                long startTime = 0;
                long endTime = 0;
                int size = reduceTask.size();
                numberReduces++;

                Iterator<Map.Entry<JobHistory.Keys, String>> kv = reduceTask.entrySet().iterator();
                for (int j = 0; j < size; j++) {
                    Map.Entry<JobHistory.Keys, String> rtc = kv.next();
                    JobHistory.Keys key = rtc.getKey();
                    String val = rtc.getValue();
                    switch (key) {
                    case START_TIME: 
                        startTime = Long.valueOf(val);
                        break;
                    case FINISH_TIME:
                        endTime = Long.valueOf(val);
                        break;
                    case COUNTERS: {
                        try {
                            Counters counters = Counters.fromEscapedCompactString(val);
                            long rows = counters.getGroup(TASK_COUNTER_GROUP)
                                    .getCounterForName(REDUCE_INPUT_RECORDS).getCounter(); 
                            if (rows < minReduceRows) minReduceRows = rows;
                            if (rows > maxReduceRows) maxReduceRows = rows;
                        } catch (ParseException e) {
                            LOG.warn("Failed to parse job counters", e);
                        }
                    }
                    break;
                    default: 
                        LOG.warn("JobHistory.Keys." + key 
                                + " : NOT INCLUDED IN PERFORMANCE ADVISOR REDUCE COUNTERS");
                        break;
                    }
                }
                
                duration = endTime - startTime;
                if (minReduceTime > duration) minReduceTime = duration;
                if (maxReduceTime < duration) maxReduceTime = duration;
                totalReduceTime += duration;

            } else if (task.get(Keys.TASK_TYPE).equals("CLEANUP")) {
                LOG.info("IGNORING TASK TYPE : " + task.get(Keys.TASK_TYPE));
            } else {
                LOG.warn("UNKNOWN TASK TYPE : " + task.get(Keys.TASK_TYPE));
            }
        }
        if (numberMaps > 0) {
            avgMapTime = (totalMapTime / numberMaps);
            mapT.put("MIN_MAP_TIME", String.valueOf(minMapTime));
            mapT.put("MAX_MAP_TIME", String.valueOf(maxMapTime));
            mapT.put("MIN_MAP_INPUT_ROWS", String.valueOf(minMapRows));
            mapT.put("MAX_MAP_INPUT_ROWS", String.valueOf(maxMapRows));
            mapT.put("AVG_MAP_TIME", String.valueOf(avgMapTime));
            mapT.put("NUMBER_MAPS", String.valueOf(numberMaps));
        }
        if (numberReduces > 0) {
            avgReduceTime = (totalReduceTime /numberReduces);
            reduceT.put("MIN_REDUCE_TIME", String.valueOf(minReduceTime));
            reduceT.put("MAX_REDUCE_TIME", String.valueOf(maxReduceTime));
            reduceT.put("AVG_REDUCE_TIME", String.valueOf(avgReduceTime));
            reduceT.put("MIN_REDUCE_INPUT_ROWS", String.valueOf(minReduceTime));
            reduceT.put("MAX_REDUCE_INPUT_ROWS", String.valueOf(maxReduceTime));            
            reduceT.put("NUMBER_REDUCES", String.valueOf(numberReduces));
        } else {
            reduceT.put("NUMBER_REDUCES", String.valueOf(0));
        }
    }
    
    /*
     * Get last successful task attempt to be added in the stats
     */
    private static Map<JobHistory.Keys, String> getLastSuccessfulTaskAttempt(
            JobHistory.Task task) {

        Map<String, JobHistory.TaskAttempt> taskAttempts = task
                .getTaskAttempts();
        int size = taskAttempts.size();
        Iterator<Map.Entry<String, JobHistory.TaskAttempt>> kv = taskAttempts
                .entrySet().iterator();
        for (int i = 0; i < size; i++) {
            // CHECK_IT: Only one SUCCESSFUL TASK ATTEMPT
            Map.Entry<String, JobHistory.TaskAttempt> tae = kv.next();
            JobHistory.TaskAttempt attempt = tae.getValue();
            if (null != attempt && null != attempt.getValues() && attempt.getValues().containsKey(JobHistory.Keys.TASK_STATUS) && attempt.getValues().get(JobHistory.Keys.TASK_STATUS).equals(
                    "SUCCESS")) {
                return attempt.getValues();
            }
        }

        return null;
    }
   
    //-------------------------------------------------------------------------
    /*
     * Job xml keys
     */
    private static final Map<String, String> XML_KEYS;
    
    static {
        XML_KEYS = new HashMap<String, String> ();       
        XML_KEYS.put("group.name", "USER_GROUP");
        XML_KEYS.put("user.name", "USER");
        XML_KEYS.put("user.dir", "HOST_DIR");
        XML_KEYS.put("mapred.job.queue.name", "QUEUE_NAME");
        XML_KEYS.put("cluster", "CLUSTER");
        XML_KEYS.put("jobName", "JOB_NAME");
        XML_KEYS.put("pig.script.id", "PIG_SCRIPT_ID");
        XML_KEYS.put("pig.script", "PIG_SCRIPT");
        XML_KEYS.put("pig.hadoop.version", "HADOOP_VERSION");
        XML_KEYS.put("pig.version", "PIG_VERSION");
        XML_KEYS.put("pig.job.feature", "PIG_JOB_FEATURE");
        XML_KEYS.put("pig.alias", "PIG_JOB_ALIAS"); 
        XML_KEYS.put("pig.parent.jobid", "PIG_JOB_PARENTS");
        XML_KEYS.put("pig.host", "HOST_NAME");        
    }
     
    public static Map<String, String> parseJobXML(InputStream in) {
        
        HashMap<String, String> xmlMap = new HashMap<String, String>();
        
        try {
            SAXParserFactory.newInstance().newSAXParser().parse(in,
                    new JobXMLHandler(xmlMap));
        } catch (Exception e) {
            LOG.warn("Failed to parser job xml", e);
        }
                          
        return xmlMap;
    }
        
    private static class JobXMLHandler extends DefaultHandler {
                  
        private static final String NAME = "name";
        private static final String VALUE = "value";
        
        private static Stack<String> tags = new Stack<String>();    
        
        private static String curTag;
        
        private static String key;
 
        private static Map<String, String> xmlMap;
                
        public JobXMLHandler(Map<String, String> xml) {
            xmlMap = xml;
        }
        
        @Override
        public void startElement(String uri, String localName,
                String qName, Attributes attributes) throws SAXException {            
            tags.add(qName);
            curTag = qName;
        }
        
        @Override
        public void endElement(String uri, String localName,
                String qName) throws SAXException {
            String tag = tags.pop();
            if (tag == null || !tag.equalsIgnoreCase(qName)) {
                throw new SAXException("Malformatted XML file: " + tag + " : "
                        + qName);
            }
            curTag = null;
        }
        
        public void characters(char[] ch, int start, int length)
                throws SAXException {
            if (tags.size() > 1) {
                String s = new String(ch, start, length); 
                if (curTag.equalsIgnoreCase(NAME)) {
                    key = s;
                }
                if (curTag.equalsIgnoreCase(VALUE)) {
                    String displayKey = XML_KEYS.get(key);
                    if (displayKey != null) {
                        xmlMap.put(displayKey, s);
                    }
                }
            }
        }
    }
}
