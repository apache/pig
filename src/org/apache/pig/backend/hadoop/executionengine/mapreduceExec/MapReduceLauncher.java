/*
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
package org.apache.pig.backend.hadoop.executionengine.mapreduceExec;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.log4j.Logger;
import org.apache.pig.impl.PigContext;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.POMapreduce;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.IndexedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.PigFile;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.impl.util.ConfigurationValidator;


/**
 * This is that Main-Class for the Pig Jar file. It will setup a Pig jar file to run with the proper
 * libraries. It will also provide a basic shell if - or -e is used as the name of the Jar file.
 * 
 * @author breed
 * 
 */
public class MapReduceLauncher {
    
    private final Log log = LogFactory.getLog(getClass());
    
    public static long totalHadoopTimeSpent = 0;
    public static int numMRJobs;
    public static int mrJobNumber;
    
    public static Configuration config = null;
    public static HExecutionEngine execEngine = null;

    public static final String LOG_DIR = "_logs";
    
    public static void setConf(Configuration configuration) {
        config = configuration;
    }
    
    public static void setExecEngine(HExecutionEngine executionEngine) {
        execEngine = executionEngine;
    }
    
    public static void initQueryStatus(int numMRJobsIn) {
        numMRJobs = numMRJobsIn;
        mrJobNumber = 0;
    }

    public static class PigWritableComparator extends WritableComparator {
        public PigWritableComparator() {
            super(Tuple.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static Random rand = new Random();

    /**
     * Submit a Pig job to hadoop.
     * 
     * @param mapFuncs
     *            a list of map functions to apply to the inputs. The cardinality of the list should
     *            be the same as input's cardinality.
     * @param groupFuncs
     *            a list of grouping functions to apply to the inputs. The cardinality of the list
     *            should be the same as input's cardinality.
     * @param reduceFunc
     *            the reduce function.
     * @param mapTasks
     *            the number of map tasks to use.
     * @param reduceTasks
     *            the number of reduce tasks to use.
     * @param input
     *            a list of inputs
     * @param output
     *            the path of the output.
     * @return an indicator of success or failure.
     * @throws IOException
     */
    public boolean launchPig(POMapreduce pom) throws IOException {
        JobConf conf = new JobConf(config);
        setJobProperties(conf, pom);
        Properties properties = pom.pigContext.getProperties();
        ConfigurationValidator.validatePigProperties(properties) ;
        String jobName = properties.getProperty(PigContext.JOB_NAME);
        conf.setJobName(jobName);
        boolean success = false;
        List<String> funcs = new ArrayList<String>();
        
        if (pom.toMap != null){
            for (EvalSpec es: pom.toMap)
                funcs.addAll(es.getFuncs());
        }
        if (pom.groupFuncs != null){
            for(EvalSpec es: pom.groupFuncs)
                funcs.addAll(es.getFuncs());
        }
        if (pom.toReduce != null) {
            funcs.addAll(pom.toReduce.getFuncs());
        }
        
        // create jobs.jar locally and pass it to hadoop
        File submitJarFile = File.createTempFile("Job", ".jar");    
        try {
            FileOutputStream fos = new FileOutputStream(submitJarFile);
            JarManager.createJar(fos, funcs, null, pom.pigContext);
            log.debug("Job jar size = " + submitJarFile.length());
            conf.setJar(submitJarFile.getPath());
            String user = System.getProperty("user.name");
            conf.setUser(user != null ? user : "Pigster");

            conf.set("pig.spill.size.threshold", 
                     properties.getProperty("pig.spill.size.threshold")) ;           
            conf.set("pig.spill.gc.activation.size", 
                    properties.getProperty("pig.spill.gc.activation.size")) ;                      
           
            if (pom.reduceParallelism != -1) {
                conf.setNumReduceTasks(pom.reduceParallelism);
            }
            if (pom.toMap != null) {
                conf.set("pig.mapFuncs", ObjectSerializer.serialize(pom.toMap));
            }
            if (pom.toCombine != null) {
                conf.set("pig.combineFunc", ObjectSerializer.serialize(pom.toCombine));
                // this is to make sure that combiner is only called once
                // since we can't handle no combine or multiple combines
                conf.setCombineOnceOnly(true);
            }
            if (pom.groupFuncs != null) {
                conf.set("pig.groupFuncs", ObjectSerializer.serialize(pom.groupFuncs));
            }
            if (pom.toReduce != null) {
                conf.set("pig.reduceFunc", ObjectSerializer.serialize(pom.toReduce));
            }
            if (pom.toSplit != null) {
                conf.set("pig.splitSpec", ObjectSerializer.serialize(pom.toSplit));
            }
            if (pom.pigContext != null) {
                conf.set("pig.pigContext", ObjectSerializer.serialize(pom.pigContext));
            }
            conf.setMapRunnerClass(PigMapReduce.class);
            if (pom.toCombine != null) {
                conf.setCombinerClass(PigCombine.class);
                //conf.setCombinerClass(PigMapReduce.class);
            }
            if (pom.quantilesFile!=null){
                conf.set("pig.quantilesFile", pom.quantilesFile);
            }
            else{
                // this is not a sort job - can use byte comparison to speed up processing
                conf.setOutputKeyComparatorClass(PigWritableComparator.class);                    
            }
            if (pom.partitionFunction!=null){
                conf.setPartitionerClass(SortPartitioner.class);
            }
            conf.setReducerClass(PigMapReduce.class);
            conf.setInputFormat(PigInputFormat.class);
            conf.setOutputFormat(PigOutputFormat.class);
            // not used starting with 0.15 conf.setInputKeyClass(Text.class);
            // not used starting with 0.15 conf.setInputValueClass(Tuple.class);
            conf.setOutputKeyClass(Tuple.class);
            if (pom.userComparator != null) {
                conf.setOutputKeyComparatorClass(pom.userComparator);
            }
            conf.setOutputValueClass(IndexedTuple.class);
            conf.set("pig.inputs", ObjectSerializer.serialize(pom.inputFileSpecs));
            
            conf.setOutputPath(new Path(pom.outputFileSpec.getFileName()));
            conf.set("pig.storeFunc",
                     ObjectSerializer.serialize(pom.outputFileSpec.getFuncSpec()));

            // Setup the DistributedCache for this job
            setupDistributedCache(pom.pigContext, conf, pom.properties, 
                                  "pig.streaming.ship.files", true);
            setupDistributedCache(pom.pigContext, conf, pom.properties, 
                                  "pig.streaming.cache.files", false);

            
            // Setup the logs directory for this job
            String jobOutputFileName = pom.pigContext.getJobOutputFile();
            if (jobOutputFileName != null && jobOutputFileName.length() > 0) {
                Path jobOutputFile = new Path(pom.pigContext.getJobOutputFile());
                conf.set("pig.output.dir", 
                        jobOutputFile.getParent().toString());
                conf.set("pig.streaming.log.dir", 
                        new Path(jobOutputFile, LOG_DIR).toString());
            }

            //
            // Now, actually submit the job (using the submit name)
            //
            JobClient jobClient = execEngine.getJobClient();
            RunningJob status = jobClient.submitJob(conf);
            log.debug("submitted job: " + status.getJobID());
            
            long sleepTime = 1000;
            double lastQueryProgress = -1.0;
            int lastJobsQueued = -1;
            double lastMapProgress = -1.0;
            double lastReduceProgress = -1.0;
            while (true) {
                try {
                    Thread.sleep(sleepTime); } catch (Exception e) {}
                    
                    if (status.isComplete()) {
                        success = status.isSuccessful();
                        if (log.isDebugEnabled()) {
                            StringBuilder sb = new StringBuilder();
                            sb.append("Job finished ");
                            sb.append((success ? "" : "un"));
                            sb.append("successfully");
                            log.debug(sb.toString());
                        }
                        if (success) {
                            mrJobNumber++;
                        }
                        double queryProgress = ((double) mrJobNumber) / ((double) numMRJobs);
                        if (queryProgress > lastQueryProgress) {
                            if (log.isInfoEnabled()) {
                                StringBuilder sbProgress = new StringBuilder();
                                sbProgress.append("Pig progress = ");
                                sbProgress.append(((int) (queryProgress * 100)));
                                sbProgress.append("%");
                                log.info(sbProgress.toString());
                            }
                            lastQueryProgress = queryProgress;
                        }
                        break;
                    }
                    else // still running
                    {
                        double mapProgress = status.mapProgress();
                        double reduceProgress = status.reduceProgress();
                        if (lastMapProgress != mapProgress || lastReduceProgress != reduceProgress) {
                            if (log.isDebugEnabled()) {
                                StringBuilder sbProgress = new StringBuilder();
                                sbProgress.append("Hadoop job progress: Map=");
                                sbProgress.append((int) (mapProgress * 100));
                                sbProgress.append("% Reduce=");
                                sbProgress.append((int) (reduceProgress * 100));
                                sbProgress.append("%");
                                log.debug(sbProgress.toString());
                            }
                            lastMapProgress = mapProgress;
                            lastReduceProgress = reduceProgress;
                        }
                        double numJobsCompleted = mrJobNumber;
                        double thisJobProgress = (mapProgress + reduceProgress) / 2.0;
                        double queryProgress = (numJobsCompleted + thisJobProgress) / ((double) numMRJobs);
                        if (queryProgress > lastQueryProgress) {
                            if (log.isInfoEnabled()) {
                                StringBuilder sbProgress = new StringBuilder();
                                sbProgress.append("Pig progress = ");
                                sbProgress.append(((int) (queryProgress * 100)));
                                sbProgress.append("%");
                                log.info(sbProgress.toString());
                            }
                            lastQueryProgress = queryProgress;
                        }
                    }
            }

            // bug 1030028: if the input file is empty; hadoop doesn't create the output file!
            Path outputFile = conf.getOutputPath();
            String outputName = outputFile.getName();
            int colon = outputName.indexOf(':');
            if (colon != -1) {
                outputFile = new Path(outputFile.getParent(), outputName.substring(0, colon));
            }
                
            try {
                ElementDescriptor descriptor = 
                    ((HDataStorage)(pom.pigContext.getDfs())).asElement(outputFile.toString());

                if (success && !descriptor.exists()) {
                        
                    // create an empty output file
                    PigFile f = new PigFile(outputFile.toString(), false);
                    f.store(BagFactory.getInstance().newDefaultBag(), 
                            new PigStorage(), 
                            pom.pigContext);
                }
            }
            catch (DataStorageException e) {
                throw WrappedIOException.wrap("Failed to obtain descriptor for " + outputFile.toString(), e);
            }

            if (!success) {
                // go find the error messages
                getErrorMessages(jobClient.getMapTaskReports(status.getJobID()),
                        "map");
                getErrorMessages(jobClient.getReduceTaskReports(status.getJobID()),
                        "reduce");
            }
            else {
                long timeSpent = 0;
              
                // NOTE: this call is crashing due to a bug in Hadoop; the bug is known and the patch has not been applied yet.
                TaskReport[] mapReports = jobClient.getMapTaskReports(status.getJobID());
                TaskReport[] reduceReports = jobClient.getReduceTaskReports(status.getJobID());
                for (TaskReport r : mapReports) {
                    timeSpent += (r.getFinishTime() - r.getStartTime());
                }
                for (TaskReport r : reduceReports) {
                    timeSpent += (r.getFinishTime() - r.getStartTime());
                }
                totalHadoopTimeSpent += timeSpent;
            }
        }
        catch (Exception e) {
            // Do we need different handling for different exceptions
            e.printStackTrace();
            throw WrappedIOException.wrap(e);
        }
        finally {
            submitJarFile.delete();
        }
        return success;
    }

    /**
     * Copy job-specific configuration from the <code>Properties</code>
     * of the given <code>POMapreduce</code>.
     * 
     * @param job job configuration
     * @param pom <code>POMapreduce</code> to be executed
     */
    private static void setJobProperties(JobConf job, POMapreduce pom) 
    throws IOException {
        for (Map.Entry property : pom.properties.entrySet()) {
            job.set((String)property.getKey(), (String)property.getValue());
        }
    }
    
    private static void setupDistributedCache(PigContext pigContext,
                                              Configuration conf, 
                                              Properties properties, String key, 
                                              boolean shipToCluster) 
    throws IOException {
        // Turn on the symlink feature
        DistributedCache.createSymlink(conf);

        // Set up the DistributedCache for this job        
        String fileNames = properties.getProperty(key);
        if (fileNames != null) {
            String[] paths = fileNames.split(",");
            
            for (String path : paths) {
                path = path.trim();
                if (path.length() != 0) {
                    Path src = new Path(path);
                    
                    // Ensure that 'src' is a valid URI
                    URI srcURI = null;
                    try {
                        srcURI = new URI(src.toString());
                    } catch (URISyntaxException ue) {
                        throw new IOException("Invalid cache specification, " +
                        		              "file doesn't exist: " + src);
                    }
                    
                    // Ship it to the cluster if necessary and add to the
                    // DistributedCache
                    if (shipToCluster) {
                        Path dst = 
                            new Path(FileLocalizer.getTemporaryPath(null, pigContext).toString());
                        FileSystem fs = dst.getFileSystem(conf);
                        fs.copyFromLocalFile(src, dst);
                        
                        // Construct the dst#srcName uri for DistributedCache
                        URI dstURI = null;
                        try {
                            dstURI = new URI(dst.toString() + "#" + src.getName());
                        } catch (URISyntaxException ue) {
                            throw new IOException("Invalid ship specification, " +
                                                  "file doesn't exist: " + dst);
                        }
                        DistributedCache.addCacheFile(dstURI, conf);
                    } else {
                        DistributedCache.addCacheFile(srcURI, conf);
                    }
                }
            }
        }
    }
    
private void getErrorMessages(TaskReport reports[], String type)
{
    for (int i = 0; i < reports.length; i++) {
        String msgs[] = reports[i].getDiagnostics();
        StringBuilder sb = new StringBuilder("Error message from task (");
        sb.append(type);
        sb.append(") ");
        sb.append(reports[i].getTaskId());
        for (int j = 0; j < msgs.length; j++) {
            sb.append(" " + msgs[j]);
        }
        if (log.isErrorEnabled()) {
            log.error(sb.toString());
        }
    }
}

}
