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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.ComparisonFunc;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.OverwritableStoreFunc;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigException;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.SecondaryKeyPartitioner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.SkewedPartitioner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.WeightedRangePartitioner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.SchemaTupleFrontend;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.NullableBigDecimalWritable;
import org.apache.pig.impl.io.NullableBigIntegerWritable;
import org.apache.pig.impl.io.NullableBooleanWritable;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.NullableDateTimeWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullablePartitionWritable;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.pigstats.mapreduce.MRPigStatsUtil;
import org.apache.pig.tools.pigstats.mapreduce.MRScriptState;

/**
 * This is compiler class that takes an MROperPlan and converts
 * it into a JobControl object with the relevant dependency info
 * maintained. The JobControl Object is made up of Jobs each of
 * which has a JobConf. The MapReduceOper corresponds to a Job
 * and the getJobCong method returns the JobConf that is configured
 * as per the MapReduceOper
 *
 * <h2>Comparator Design</h2>
 * <p>
 * A few words on how comparators are chosen.  In almost all cases we use raw
 * comparators (the one exception being when the user provides a comparison
 * function for order by).  For order by queries the PigTYPERawComparator
 * functions are used, where TYPE is Int, Long, etc.  These comparators are
 * null aware and asc/desc aware.  The first byte of each of the
 * NullableTYPEWritable classes contains info on whether the value is null.
 * Asc/desc is written as an array into the JobConf with the key pig.sortOrder
 * so that it can be read by each of the comparators as part of their
 * setConf call.
 * <p>
 * For non-order by queries, PigTYPEWritableComparator classes are used.
 * These are all just type specific instances of WritableComparator.
 *
 */
@SuppressWarnings("deprecation")
public class JobControlCompiler{
    MROperPlan plan;
    Configuration conf;
    Configuration defaultConf;
    PigContext pigContext;

    private static final Matcher DISTRIBUTED_CACHE_ARCHIVE_MATCHER = Pattern
            .compile("\\.(zip|tgz|tar\\.gz|tar)$").matcher("");

    private static final Log log = LogFactory.getLog(JobControlCompiler.class);

    public static final String LOG_DIR = "_logs";

    public static final String END_OF_INP_IN_MAP = "pig.invoke.close.in.map";

    private static final String REDUCER_ESTIMATOR_KEY = "pig.exec.reducer.estimator";
    private static final String REDUCER_ESTIMATOR_ARG_KEY =  "pig.exec.reducer.estimator.arg";

    public static final String PIG_MAP_COUNTER = "pig.counters.counter_";
    public static final String PIG_MAP_RANK_NAME = "pig.rank_";
    public static final String PIG_MAP_SEPARATOR = "_";
    public HashMap<String, ArrayList<Pair<String,Long>>> globalCounters = new HashMap<String, ArrayList<Pair<String,Long>>>();

    public static final String SMALL_JOB_LOG_MSG = "This job was detected as a small job, will run in-process instead";
    public static final String BIG_JOB_LOG_MSG = "This job cannot be converted run in-process";

    /**
     * We will serialize the POStore(s) present in map and reduce in lists in
     * the Hadoop Conf. In the case of Multi stores, we could deduce these from
     * the map plan and reduce plan but in the case of single store, we remove
     * the POStore from the plan - in either case, we serialize the POStore(s)
     * so that PigOutputFormat and PigOutputCommiter can get the POStore(s) in
     * the same way irrespective of whether it is multi store or single store.
     */
    public static final String PIG_MAP_STORES = "pig.map.stores";
    public static final String PIG_REDUCE_STORES = "pig.reduce.stores";

    // A mapping of job to pair of store locations and tmp locations for that job
    private Map<Job, Pair<List<POStore>, Path>> jobStoreMap;

    private Map<Job, MapReduceOper> jobMroMap;
    private int counterSize;

    public JobControlCompiler(PigContext pigContext, Configuration conf) {
        this(pigContext, conf, null);
    }

    public JobControlCompiler(PigContext pigContext, Configuration conf, Configuration defaultConf) {
        this.pigContext = pigContext;
        this.conf = conf;
        this.defaultConf = defaultConf;
        jobStoreMap = new HashMap<Job, Pair<List<POStore>, Path>>();
        jobMroMap = new HashMap<Job, MapReduceOper>();
    }

    /**
     * Returns all store locations of a previously compiled job
     */
    public List<POStore> getStores(Job job) {
        Pair<List<POStore>, Path> pair = jobStoreMap.get(job);
        if (pair != null && pair.first != null) {
            return pair.first;
        } else {
            return new ArrayList<POStore>();
        }
    }

    /**
     * Resets the state
     */
    public void reset() {
        jobStoreMap = new HashMap<Job, Pair<List<POStore>, Path>>();
        jobMroMap = new HashMap<Job, MapReduceOper>();
        UDFContext.getUDFContext().reset();
    }

    /**
     * Gets the map of Job and the MR Operator
     */
    public Map<Job, MapReduceOper> getJobMroMap() {
        return Collections.unmodifiableMap(jobMroMap);
    }

    /**
     * Moves all the results of a collection of MR jobs to the final
     * output directory. Some of the results may have been put into a
     * temp location to work around restrictions with multiple output
     * from a single map reduce job.
     *
     * This method should always be called after the job execution
     * completes.
     */
    public void moveResults(List<Job> completedJobs) throws IOException {
        for (Job job: completedJobs) {
            Pair<List<POStore>, Path> pair = jobStoreMap.get(job);
            if (pair != null && pair.second != null) {
                Path tmp = pair.second;
                Path abs = new Path(tmp, "abs");
                Path rel = new Path(tmp, "rel");
                FileSystem fs = tmp.getFileSystem(conf);

                if (fs.exists(abs)) {
                    moveResults(abs, abs.toUri().getPath(), fs);
                }

                if (fs.exists(rel)) {
                    moveResults(rel, rel.toUri().getPath()+"/", fs);
                }
            }
        }
    }

    /**
     * Walks the temporary directory structure to move (rename) files
     * to their final location.
     */
    private void moveResults(Path p, String rem, FileSystem fs) throws IOException {
        for (FileStatus fstat: fs.listStatus(p)) {
            Path src = fstat.getPath();
            if (fstat.isDir()) {
                log.info("mkdir: "+src);
                fs.mkdirs(removePart(src, rem));
                moveResults(fstat.getPath(), rem, fs);
            } else {
                Path dst = removePart(src, rem);
                log.info("mv: "+src+" "+dst);
                fs.rename(src,dst);
            }
        }
    }

    private Path removePart(Path src, String part) {
        URI uri = src.toUri();
        String pathStr = uri.getPath().replace(part, "");
        return new Path(pathStr);
    }

    /**
     * Compiles all jobs that have no dependencies removes them from
     * the plan and returns. Should be called with the same plan until
     * exhausted.
     * @param plan - The MROperPlan to be compiled
     * @param grpName - The name given to the JobControl
     * @return JobControl object - null if no more jobs in plan
     * @throws JobCreationException
     */
    public JobControl compile(MROperPlan plan, String grpName) throws JobCreationException{
        // Assert plan.size() != 0
        this.plan = plan;

        int timeToSleep;
        String defaultPigJobControlSleep = pigContext.getExecType().isLocal() ? "100" : "5000";
        String pigJobControlSleep = conf.get("pig.jobcontrol.sleep", defaultPigJobControlSleep);
        if (!pigJobControlSleep.equals(defaultPigJobControlSleep)) {
            log.info("overriding default JobControl sleep (" + defaultPigJobControlSleep + ") to " + pigJobControlSleep);
        }

        try {
            timeToSleep = Integer.parseInt(pigJobControlSleep);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid configuration " +
                    "pig.jobcontrol.sleep=" + pigJobControlSleep +
                    " should be a time in ms. default=" + defaultPigJobControlSleep, e);
        }

        JobControl jobCtrl = HadoopShims.newJobControl(grpName, timeToSleep);

        try {
            List<MapReduceOper> roots = new LinkedList<MapReduceOper>();
            roots.addAll(plan.getRoots());
            for (MapReduceOper mro: roots) {
                if(mro instanceof NativeMapReduceOper) {
                    return null;
                }
                Job job = getJob(plan, mro, conf, pigContext);
                jobMroMap.put(job, mro);
                jobCtrl.addJob(job);
            }
        } catch (JobCreationException jce) {
            throw jce;
        } catch(Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
        }

        return jobCtrl;
    }

    // Update Map-Reduce plan with the execution status of the jobs. If one job
    // completely fail (the job has only one store and that job fail), then we
    // remove all its dependent jobs. This method will return the number of MapReduceOper
    // removed from the Map-Reduce plan
    public int updateMROpPlan(List<Job> completeFailedJobs)
    {
        int sizeBefore = plan.size();
        for (Job job : completeFailedJobs)  // remove all subsequent jobs
        {
            MapReduceOper mrOper = jobMroMap.get(job);
            plan.trimBelow(mrOper);
            plan.remove(mrOper);
        }

        // Remove successful jobs from jobMroMap
        for (Job job : jobMroMap.keySet())
        {
            if (!completeFailedJobs.contains(job))
            {
                MapReduceOper mro = jobMroMap.get(job);
                if (!pigContext.inIllustrator && mro.isCounterOperation())
                    saveCounters(job,mro.getOperationID());
                plan.remove(mro);
            }
        }
        jobMroMap.clear();
        int sizeAfter = plan.size();
        return sizeBefore-sizeAfter;
    }

    /**
     * Reads the global counters produced by a job on the group labeled with PIG_MAP_RANK_NAME.
     * Then, it is calculated the cumulative sum, which consists on the sum of previous cumulative
     * sum plus the previous global counter value.
     * @param job with the global counters collected.
     * @param operationID After being collected on global counters (POCounter),
     * these values are passed via configuration file to PORank, by using the unique
     * operation identifier
     */
    private void saveCounters(Job job, String operationID) {
        Counters counters;
        Group groupCounters;

        Long previousValue = 0L;
        Long previousSum = 0L;
        ArrayList<Pair<String,Long>> counterPairs;

        try {
            counters = HadoopShims.getCounters(job);

            String groupName = getGroupName(counters.getGroupNames());
            // In case that the counter group was not find, we need to find
            // out why. Only acceptable state is that the relation has been
            // empty.
            if (groupName == null) {
                Counter outputRecords =
                    counters.getGroup(MRPigStatsUtil.TASK_COUNTER_GROUP)
                    .getCounterForName(MRPigStatsUtil.MAP_OUTPUT_RECORDS);

                if(outputRecords.getCounter() == 0) {
                    globalCounters.put(operationID, new ArrayList<Pair<String, Long>>());
                    return;
                } else {
                  throw new RuntimeException("Did not found RANK counter group for operationId: " + operationID);
                }
            }
            groupCounters = counters.getGroup(groupName);

            Iterator<Counter> it = groupCounters.iterator();
            HashMap<Integer,Long> counterList = new HashMap<Integer, Long>();

            while(it.hasNext()) {
                try{
                    Counter c = it.next();
                    counterList.put(Integer.valueOf(c.getDisplayName()), c.getValue());
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            counterSize = counterList.size();
            counterPairs = new ArrayList<Pair<String,Long>>();

            for(int i = 0; i < counterSize; i++){
                previousSum += previousValue;
                previousValue = counterList.get(Integer.valueOf(i));
                counterPairs.add(new Pair<String, Long>(JobControlCompiler.PIG_MAP_COUNTER + operationID + JobControlCompiler.PIG_MAP_SEPARATOR + i, previousSum));
            }

            globalCounters.put(operationID, counterPairs);

        } catch (Exception e) {
            String msg = "Error to read counters into Rank operation counterSize "+counterSize;
            throw new RuntimeException(msg, e);
        }
    }

    private String getGroupName(Collection<String> collection) {
        for (String name : collection) {
            if (name.contains(PIG_MAP_RANK_NAME))
                return name;
        }
        return null;
    }

    private boolean okToRunLocal(org.apache.hadoop.mapreduce.Job job, MapReduceOper mro, List<POLoad> lds) throws IOException {
        Configuration conf = job.getConfiguration();
        if(!conf.getBoolean(PigConfiguration.PIG_AUTO_LOCAL_ENABLED, false)) {
            return false;
        }

        long totalInputFileSize = InputSizeReducerEstimator.getTotalInputFileSize(conf, lds, job);
        long inputByteMax = conf.getLong(PigConfiguration.PIG_AUTO_LOCAL_INPUT_MAXBYTES, 100*1000*1000l);
        log.info("Size of input: " + totalInputFileSize +" bytes. Small job threshold: " + inputByteMax );
        if (totalInputFileSize < 0 || totalInputFileSize > inputByteMax) {
            return false;
        }

        int reducers = conf.getInt("mapred.reduce.tasks", 1);
        log.info("No of reducers: " + reducers);
        if (reducers > 1) {
            return false;
        }

        return true;
    }

    /**
     * The method that creates the Job corresponding to a MapReduceOper.
     * The assumption is that
     * every MapReduceOper will have a load and a store. The JobConf removes
     * the load operator and serializes the input filespec so that PigInputFormat can
     * take over the creation of splits. It also removes the store operator
     * and serializes the output filespec so that PigOutputFormat can take over
     * record writing. The remaining portion of the map plan and reduce plans are
     * serialized and stored for the PigMapReduce or PigMapOnly objects to take over
     * the actual running of the plans.
     * The Mapper &amp; Reducer classes and the required key value formats are set.
     * Checks if this is a map only job and uses PigMapOnly class as the mapper
     * and uses PigMapReduce otherwise.
     * If it is a Map Reduce job, it is bound to have a package operator. Remove it from
     * the reduce plan and serializes it so that the PigMapReduce class can use it to package
     * the indexed tuples received by the reducer.
     * @param mro - The MapReduceOper for which the JobConf is required
     * @param config - the Configuration object from which JobConf is built
     * @param pigContext - The PigContext passed on from execution engine
     * @return Job corresponding to mro
     * @throws JobCreationException
     */
    @SuppressWarnings({ "unchecked" })
    private Job getJob(MROperPlan plan, MapReduceOper mro, Configuration config, PigContext pigContext) throws JobCreationException{
        org.apache.hadoop.mapreduce.Job nwJob = null;

        try{
            nwJob = new org.apache.hadoop.mapreduce.Job(config);
        }catch(Exception e) {
            throw new JobCreationException(e);
        }

        Configuration conf = nwJob.getConfiguration();

        ArrayList<FileSpec> inp = new ArrayList<FileSpec>();
        ArrayList<List<OperatorKey>> inpTargets = new ArrayList<List<OperatorKey>>();
        ArrayList<String> inpSignatureLists = new ArrayList<String>();
        ArrayList<Long> inpLimits = new ArrayList<Long>();
        ArrayList<POStore> storeLocations = new ArrayList<POStore>();
        Path tmpLocation = null;

        // add settings for pig statistics
        String setScriptProp = conf.get(PigConfiguration.INSERT_ENABLED, "true");
        if (setScriptProp.equalsIgnoreCase("true")) {
            MRScriptState ss = MRScriptState.get();
            ss.addSettingsToConf(mro, conf);
        }

        conf.set("mapred.mapper.new-api", "true");
        conf.set("mapred.reducer.new-api", "true");

        String buffPercent = conf.get("mapred.job.reduce.markreset.buffer.percent");
        if (buffPercent == null || Double.parseDouble(buffPercent) <= 0) {
            log.info("mapred.job.reduce.markreset.buffer.percent is not set, set to default 0.3");
            conf.set("mapred.job.reduce.markreset.buffer.percent", "0.3");
        }else{
            log.info("mapred.job.reduce.markreset.buffer.percent is set to " + conf.get("mapred.job.reduce.markreset.buffer.percent"));
        }

        configureCompression(conf);

        try{
            //Process the POLoads
            List<POLoad> lds = PlanHelper.getPhysicalOperators(mro.mapPlan, POLoad.class);

            if(lds!=null && lds.size()>0){
                for (POLoad ld : lds) {
                    LoadFunc lf = ld.getLoadFunc();
                    lf.setLocation(ld.getLFile().getFileName(), nwJob);

                    //Store the inp filespecs
                    inp.add(ld.getLFile());
                }
            }

            if(!mro.reducePlan.isEmpty()){
                log.info("Reduce phase detected, estimating # of required reducers.");
                adjustNumReducers(plan, mro, nwJob);
            } else {
                nwJob.setNumReduceTasks(0);
            }

            if(lds!=null && lds.size()>0){
                for (POLoad ld : lds) {
                    //Store the target operators for tuples read
                    //from this input
                    List<PhysicalOperator> ldSucs = mro.mapPlan.getSuccessors(ld);
                    List<OperatorKey> ldSucKeys = new ArrayList<OperatorKey>();
                    if(ldSucs!=null){
                        for (PhysicalOperator operator2 : ldSucs) {
                            ldSucKeys.add(operator2.getOperatorKey());
                        }
                    }
                    inpTargets.add(ldSucKeys);
                    inpSignatureLists.add(ld.getSignature());
                    inpLimits.add(ld.getLimit());
                    //Remove the POLoad from the plan
                    if (!pigContext.inIllustrator)
                        mro.mapPlan.remove(ld);
                }
            }

            if (!pigContext.inIllustrator && ! pigContext.getExecType().isLocal())
            {
                if (okToRunLocal(nwJob, mro, lds)) {
                    log.info(SMALL_JOB_LOG_MSG);
                    // override with the default conf to run in local mode
                    for (Entry<String, String> entry : defaultConf) {
                        String key = entry.getKey();
                        if (key.equals("mapred.reduce.tasks") || key.equals("mapreduce.job.reduces")) {
                            // this must not be set back to the default in case it has been set to 0 for example.
                            continue;
                        }
                        if (key.startsWith("fs.")) {
                            // we don't want to change fs settings back
                            continue;
                        }
                        if (key.startsWith("io.")) {
                            // we don't want to change io settings back
                            continue;
                        }
                        String value = entry.getValue();
                        if (conf.get(key) == null || !conf.get(key).equals(value)) {
                            conf.set(key, value);
                        }
                    }

                    conf.setBoolean(PigImplConstants.CONVERTED_TO_LOCAL, true);
                } else {
                    log.info(BIG_JOB_LOG_MSG);
                    // Setup the DistributedCache for this job
                    for (URL extraJar : pigContext.extraJars) {
                        log.debug("Adding jar to DistributedCache: " + extraJar.toString());
                        putJarOnClassPathThroughDistributedCache(pigContext, conf, extraJar);
                    }

                    for (String scriptJar : pigContext.scriptJars) {
                        log.debug("Adding jar to DistributedCache: " + scriptJar.toString());
                        putJarOnClassPathThroughDistributedCache(pigContext, conf, new File(scriptJar).toURI().toURL());
                    }

                    //Create the jar of all functions and classes required
                    File submitJarFile = File.createTempFile("Job", ".jar");
                    log.info("creating jar file "+submitJarFile.getName());
                    // ensure the job jar is deleted on exit
                    submitJarFile.deleteOnExit();
                    FileOutputStream fos = new FileOutputStream(submitJarFile);
                    JarManager.createJar(fos, mro.UDFs, pigContext);
                    log.info("jar file "+submitJarFile.getName()+" created");
                    //Start setting the JobConf properties
                    conf.set("mapred.jar", submitJarFile.getPath());
                }
            }

            if(Utils.isLocal(pigContext, conf)) {
                ConfigurationUtil.replaceConfigForLocalMode(conf);
            }
            conf.set("pig.inputs", ObjectSerializer.serialize(inp));
            conf.set("pig.inpTargets", ObjectSerializer.serialize(inpTargets));
            conf.set("pig.inpSignatures", ObjectSerializer.serialize(inpSignatureLists));
            conf.set("pig.inpLimits", ObjectSerializer.serialize(inpLimits));
            conf.set("pig.pigContext", ObjectSerializer.serialize(pigContext));
            conf.set("udf.import.list", ObjectSerializer.serialize(PigContext.getPackageImportList()));
            // this is for unit tests since some don't create PigServer

            // if user specified the job name using -D switch, Pig won't reset the name then.
            if (System.getProperty("mapred.job.name") == null &&
                    pigContext.getProperties().getProperty(PigContext.JOB_NAME) != null){
                nwJob.setJobName(pigContext.getProperties().getProperty(PigContext.JOB_NAME));
            }

            if (pigContext.getProperties().getProperty(PigContext.JOB_PRIORITY) != null) {
                // If the job priority was set, attempt to get the corresponding enum value
                // and set the hadoop job priority.
                String jobPriority = pigContext.getProperties().getProperty(PigContext.JOB_PRIORITY).toUpperCase();
                try {
                    // Allow arbitrary case; the Hadoop job priorities are all upper case.
                    conf.set("mapred.job.priority", JobPriority.valueOf(jobPriority).toString());

                } catch (IllegalArgumentException e) {
                    StringBuffer sb = new StringBuffer("The job priority must be one of [");
                    JobPriority[] priorities = JobPriority.values();
                    for (int i = 0; i < priorities.length; ++i) {
                        if (i > 0)  sb.append(", ");
                        sb.append(priorities[i]);
                    }
                    sb.append("].  You specified [" + jobPriority + "]");
                    throw new JobCreationException(sb.toString());
                }
            }

            setupDistributedCache(pigContext, conf, pigContext.getProperties(),
                    "pig.streaming.ship.files", true);
            setupDistributedCache(pigContext, conf, pigContext.getProperties(),
                    "pig.streaming.cache.files", false);

            nwJob.setInputFormatClass(PigInputFormat.class);

            // tmp file compression setups
            // PIG-3741 This must be done before setStoreLocation on POStores
            Utils.setTmpFileCompressionOnConf(pigContext, conf);

            //Process POStore and remove it from the plan
            LinkedList<POStore> mapStores = PlanHelper.getPhysicalOperators(mro.mapPlan, POStore.class);
            LinkedList<POStore> reduceStores = PlanHelper.getPhysicalOperators(mro.reducePlan, POStore.class);

            for (POStore st : mapStores) {
                storeLocations.add(st);
                StoreFuncInterface sFunc = st.getStoreFunc();
                sFunc.setStoreLocation(st.getSFile().getFileName(), nwJob);
                if (sFunc instanceof OverwritableStoreFunc) {
                    OverwritableStoreFunc osf = (OverwritableStoreFunc) sFunc;
                    if (osf.shouldOverwrite()) {
                        osf.cleanupOutput(st, nwJob);
                    }
                }
            }

            for (POStore st : reduceStores) {
                storeLocations.add(st);
                StoreFuncInterface sFunc = st.getStoreFunc();
                sFunc.setStoreLocation(st.getSFile().getFileName(), nwJob);
                if (sFunc instanceof OverwritableStoreFunc) {
                    OverwritableStoreFunc osf = (OverwritableStoreFunc) sFunc;
                    if (osf.shouldOverwrite()) {
                        osf.cleanupOutput(st, nwJob);
                    }
                }
            }

            setOutputFormat(nwJob);

            if (mapStores.size() + reduceStores.size() == 1) { // single store case
                log.info("Setting up single store job");

                POStore st;
                if (reduceStores.isEmpty()) {
                    st = mapStores.get(0);
                    if(!pigContext.inIllustrator)
                        mro.mapPlan.remove(st);
                }
                else {
                    st = reduceStores.get(0);
                    if(!pigContext.inIllustrator)
                        mro.reducePlan.remove(st);
                }

                MapRedUtil.setupStreamingDirsConfSingle(st, pigContext, conf);
            }
            else if (mapStores.size() + reduceStores.size() > 0) { // multi store case
                log.info("Setting up multi store job");
                MapRedUtil.setupStreamingDirsConfMulti(pigContext, conf);

                boolean disableCounter = conf.getBoolean("pig.disable.counter", false);
                if (disableCounter) {
                    log.info("Disable Pig custom output counters");
                }
                int idx = 0;
                for (POStore sto: storeLocations) {
                    sto.setDisableCounter(disableCounter);
                    sto.setMultiStore(true);
                    sto.setIndex(idx++);
                }
            }

            // store map key type
            // this is needed when the key is null to create
            // an appropriate NullableXXXWritable object
            conf.set("pig.map.keytype", ObjectSerializer.serialize(new byte[] { mro.mapKeyType }));

            // set parent plan in all operators in map and reduce plans
            // currently the parent plan is really used only when POStream is present in the plan
            new PhyPlanSetter(mro.mapPlan).visit();
            new PhyPlanSetter(mro.reducePlan).visit();

            // this call modifies the ReplFiles names of POFRJoin operators
            // within the MR plans, must be called before the plans are
            // serialized
            setupDistributedCacheForJoin(mro, pigContext, conf);

            // Search to see if we have any UDFs that need to pack things into the
            // distributed cache.
            setupDistributedCacheForUdfs(mro, pigContext, conf);

            SchemaTupleFrontend.copyAllGeneratedToDistributedCache(pigContext, conf);

            POPackage pack = null;
            if(mro.reducePlan.isEmpty()){
                //MapOnly Job
                nwJob.setMapperClass(PigMapOnly.Map.class);
                if(!pigContext.inIllustrator)
                    conf.set("pig.mapPlan", ObjectSerializer.serialize(mro.mapPlan));
                if(mro.isEndOfAllInputSetInMap()) {
                    // this is used in Map.close() to decide whether the
                    // pipeline needs to be rerun one more time in the close()
                    // The pipeline is rerun if there either was a stream or POMergeJoin
                    conf.set(END_OF_INP_IN_MAP, "true");
                }
            }
            else{
                //Map Reduce Job
                //Process the POPackage operator and remove it from the reduce plan
                if(!mro.combinePlan.isEmpty()){
                    POPackage combPack = (POPackage)mro.combinePlan.getRoots().get(0);
                    mro.combinePlan.remove(combPack);
                    nwJob.setCombinerClass(PigCombiner.Combine.class);
                    conf.set("pig.combinePlan", ObjectSerializer.serialize(mro.combinePlan));
                    conf.set("pig.combine.package", ObjectSerializer.serialize(combPack));
                } else if (mro.needsDistinctCombiner()) {
                    nwJob.setCombinerClass(DistinctCombiner.Combine.class);
                    log.info("Setting identity combiner class.");
                }
                pack = (POPackage)mro.reducePlan.getRoots().get(0);
                if(!pigContext.inIllustrator)
                    mro.reducePlan.remove(pack);
                nwJob.setMapperClass(PigMapReduce.Map.class);
                nwJob.setReducerClass(PigMapReduce.Reduce.class);

                if (mro.customPartitioner != null)
                    nwJob.setPartitionerClass(PigContext.resolveClassName(mro.customPartitioner));

                if(!pigContext.inIllustrator)
                    conf.set("pig.mapPlan", ObjectSerializer.serialize(mro.mapPlan));
                if(mro.isEndOfAllInputSetInMap()) {
                    // this is used in Map.close() to decide whether the
                    // pipeline needs to be rerun one more time in the close()
                    // The pipeline is rerun only if there was a stream or merge-join.
                    conf.set(END_OF_INP_IN_MAP, "true");
                }
                if(!pigContext.inIllustrator)
                    conf.set("pig.reducePlan", ObjectSerializer.serialize(mro.reducePlan));
                if(mro.isEndOfAllInputSetInReduce()) {
                    // this is used in Map.close() to decide whether the
                    // pipeline needs to be rerun one more time in the close()
                    // The pipeline is rerun only if there was a stream
                    conf.set("pig.stream.in.reduce", "true");
                }
                if (!pigContext.inIllustrator)
                    conf.set("pig.reduce.package", ObjectSerializer.serialize(pack));
                conf.set("pig.reduce.key.type",
                        Byte.toString(pack.getPkgr().getKeyType()));

                if (mro.getUseSecondaryKey()) {
                    nwJob.setGroupingComparatorClass(PigSecondaryKeyGroupComparator.class);
                    nwJob.setPartitionerClass(SecondaryKeyPartitioner.class);
                    nwJob.setSortComparatorClass(PigSecondaryKeyComparator.class);
                    nwJob.setOutputKeyClass(NullableTuple.class);
                    conf.set("pig.secondarySortOrder",
                            ObjectSerializer.serialize(mro.getSecondarySortOrder()));

                }
                else
                {
                    Class<? extends WritableComparable> keyClass = HDataType
                            .getWritableComparableTypes(
                                    pack.getPkgr().getKeyType()).getClass();
                    nwJob.setOutputKeyClass(keyClass);
                    selectComparator(mro, pack.getPkgr().getKeyType(), nwJob);
                }
                nwJob.setOutputValueClass(NullableTuple.class);
            }

            if(mro.isGlobalSort() || mro.isLimitAfterSort()){
                if (mro.isGlobalSort()) {
                    String symlink = addSingleFileToDistributedCache(
                            pigContext, conf, mro.getQuantFile(), "pigsample");
                    conf.set("pig.quantilesFile", symlink);
                    nwJob.setPartitionerClass(WeightedRangePartitioner.class);
                }

                if (mro.isUDFComparatorUsed) {
                    boolean usercomparator = false;
                    for (String compFuncSpec : mro.UDFs) {
                        Class comparator = PigContext.resolveClassName(compFuncSpec);
                        if(ComparisonFunc.class.isAssignableFrom(comparator)) {
                            nwJob.setMapperClass(PigMapReduce.MapWithComparator.class);
                            nwJob.setReducerClass(PigMapReduce.ReduceWithComparator.class);
                            conf.set("pig.reduce.package", ObjectSerializer.serialize(pack));
                            conf.set("pig.usercomparator", "true");
                            nwJob.setOutputKeyClass(NullableTuple.class);
                            nwJob.setSortComparatorClass(comparator);
                            usercomparator = true;
                            break;
                        }
                    }
                    if (!usercomparator) {
                        String msg = "Internal error. Can't find the UDF comparator";
                        throw new IOException (msg);
                    }

                } else {
                    conf.set("pig.sortOrder",
                            ObjectSerializer.serialize(mro.getSortOrder()));
                }
            }

            if (mro.isSkewedJoin()) {
                String symlink = addSingleFileToDistributedCache(pigContext,
                        conf, mro.getSkewedJoinPartitionFile(), "pigdistkey");
                conf.set("pig.keyDistFile", symlink);
                nwJob.setPartitionerClass(SkewedPartitioner.class);
                nwJob.setMapperClass(PigMapReduce.MapWithPartitionIndex.class);
                nwJob.setMapOutputKeyClass(NullablePartitionWritable.class);
                nwJob.setGroupingComparatorClass(PigGroupingPartitionWritableComparator.class);
            }

            if (mro.isCounterOperation()) {
                if (mro.isRowNumber()) {
                    nwJob.setMapperClass(PigMapReduceCounter.PigMapCounter.class);
                } else {
                    nwJob.setReducerClass(PigMapReduceCounter.PigReduceCounter.class);
                }
            }

            if(mro.isRankOperation()) {
                Iterator<String> operationIDs = mro.getRankOperationId().iterator();

                while(operationIDs.hasNext()) {
                    String operationID = operationIDs.next();
                    Iterator<Pair<String, Long>> itPairs = globalCounters.get(operationID).iterator();
                    Pair<String,Long> pair = null;
                    while(itPairs.hasNext()) {
                        pair = itPairs.next();
                        conf.setLong(pair.first, pair.second);
                    }
                }
            }

            if (!pigContext.inIllustrator)
            {
                // unset inputs for POStore, otherwise, map/reduce plan will be unnecessarily deserialized
                for (POStore st: mapStores) { st.setInputs(null); st.setParentPlan(null);}
                for (POStore st: reduceStores) { st.setInputs(null); st.setParentPlan(null);}
                conf.set(PIG_MAP_STORES, ObjectSerializer.serialize(mapStores));
                conf.set(PIG_REDUCE_STORES, ObjectSerializer.serialize(reduceStores));
            }

            String tmp;
            long maxCombinedSplitSize = 0;
            if (!mro.combineSmallSplits() || pigContext.getProperties().getProperty("pig.splitCombination", "true").equals("false"))
                conf.setBoolean("pig.noSplitCombination", true);
            else if ((tmp = pigContext.getProperties().getProperty("pig.maxCombinedSplitSize", null)) != null) {
                try {
                    maxCombinedSplitSize = Long.parseLong(tmp);
                } catch (NumberFormatException e) {
                    log.warn("Invalid numeric format for pig.maxCombinedSplitSize; use the default maximum combined split size");
                }
            }
            if (maxCombinedSplitSize > 0)
                conf.setLong("pig.maxCombinedSplitSize", maxCombinedSplitSize);

            // It's a hack to set distributed cache file for hadoop 23. Once MiniMRCluster do not require local
            // jar on fixed location, this can be removed
            if (pigContext.getExecType() == ExecType.MAPREDUCE) {
                String newfiles = conf.get("alternative.mapreduce.job.cache.files");
                if (newfiles!=null) {
                    String files = conf.get("mapreduce.job.cache.files");
                    conf.set("mapreduce.job.cache.files",
                            files == null ? newfiles.toString() : files + "," + newfiles);
                }
            }
            // Serialize the UDF specific context info.
            UDFContext.getUDFContext().serialize(conf);
            Job cjob = new Job(new JobConf(conf), new ArrayList<Job>());
            jobStoreMap.put(cjob,new Pair<List<POStore>, Path>(storeLocations, tmpLocation));
            return cjob;

        } catch (JobCreationException jce) {
            throw jce;
        } catch(Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
        }
    }

    public static void configureCompression(Configuration conf) {
        // Convert mapred.output.* to output.compression.*, See PIG-1791
        if( "true".equals( conf.get( "mapred.output.compress" ) ) ) {
            conf.set( "output.compression.enabled",  "true" );
            String codec = conf.get( "mapred.output.compression.codec" );
            if( codec == null ) {
                throw new IllegalArgumentException("'mapred.output.compress' is set but no value is specified for 'mapred.output.compression.codec'." );
            } else {
                conf.set( "output.compression.codec", codec );
            }
        }
    }

    /**
     * Adjust the number of reducers based on the default_parallel, requested parallel and estimated
     * parallel. For sampler jobs, we also adjust the next job in advance to get its runtime parallel as
     * the number of partitions used in the sampler.
     * @param plan the MR plan
     * @param mro the MR operator
     * @param nwJob the current job
     * @throws IOException
     */
    public void adjustNumReducers(MROperPlan plan, MapReduceOper mro,
            org.apache.hadoop.mapreduce.Job nwJob) throws IOException {
        int jobParallelism = calculateRuntimeReducers(mro, nwJob);

        if (mro.isSampler() && plan.getSuccessors(mro) != null) {
            // We need to calculate the final number of reducers of the next job (order-by or skew-join)
            // to generate the quantfile.
            MapReduceOper nextMro = plan.getSuccessors(mro).get(0);

            // Here we use the same conf and Job to calculate the runtime #reducers of the next job
            // which is fine as the statistics comes from the nextMro's POLoads
            int nPartitions = calculateRuntimeReducers(nextMro, nwJob);

            // set the runtime #reducer of the next job as the #partition
            ParallelConstantVisitor visitor =
                    new ParallelConstantVisitor(mro.reducePlan, nPartitions);
            visitor.visit();
        }
        log.info("Setting Parallelism to " + jobParallelism);

        Configuration conf = nwJob.getConfiguration();

        // set various parallelism into the job conf for later analysis, PIG-2779
        conf.setInt("pig.info.reducers.default.parallel", pigContext.defaultParallel);
        conf.setInt("pig.info.reducers.requested.parallel", mro.requestedParallelism);
        conf.setInt("pig.info.reducers.estimated.parallel", mro.estimatedParallelism);

        // this is for backward compatibility, and we encourage to use runtimeParallelism at runtime
        mro.requestedParallelism = jobParallelism;

        // finally set the number of reducers
        conf.setInt("mapred.reduce.tasks", jobParallelism);
    }

    /**
     * Calculate the runtime #reducers based on the default_parallel, requested parallel and estimated
     * parallel, and save it to MapReduceOper's runtimeParallelism.
     * @return the runtimeParallelism
     * @throws IOException
     */
    private int calculateRuntimeReducers(MapReduceOper mro,
            org.apache.hadoop.mapreduce.Job nwJob) throws IOException{
        // we don't recalculate for the same job
        if (mro.runtimeParallelism != -1) {
            return mro.runtimeParallelism;
        }

        int jobParallelism = -1;

        if (mro.requestedParallelism > 0) {
            jobParallelism = mro.requestedParallelism;
        } else if (pigContext.defaultParallel > 0) {
            jobParallelism = pigContext.defaultParallel;
        } else {
            mro.estimatedParallelism = estimateNumberOfReducers(nwJob, mro);
            if (mro.estimatedParallelism > 0) {
                jobParallelism = mro.estimatedParallelism;
            } else {
                // reducer estimation could return -1 if it couldn't estimate
                log.info("Could not estimate number of reducers and no requested or default " +
                        "parallelism set. Defaulting to 1 reducer.");
                jobParallelism = 1;
            }
        }

        // save it
        mro.runtimeParallelism = jobParallelism;
        return jobParallelism;
    }

    /**
     * Looks up the estimator from REDUCER_ESTIMATOR_KEY and invokes it to find the number of
     * reducers to use. If REDUCER_ESTIMATOR_KEY isn't set, defaults to InputSizeReducerEstimator.
     * @param job
     * @param mapReducerOper
     * @throws IOException
     */
    public static int estimateNumberOfReducers(org.apache.hadoop.mapreduce.Job job,
            MapReduceOper mapReducerOper) throws IOException {
        Configuration conf = job.getConfiguration();

        PigReducerEstimator estimator = conf.get(REDUCER_ESTIMATOR_KEY) == null ?
                new InputSizeReducerEstimator() :
                    PigContext.instantiateObjectFromParams(conf,
                            REDUCER_ESTIMATOR_KEY, REDUCER_ESTIMATOR_ARG_KEY, PigReducerEstimator.class);

                log.info("Using reducer estimator: " + estimator.getClass().getName());
                int numberOfReducers = estimator.estimateNumberOfReducers(job, mapReducerOper);
                return numberOfReducers;
    }

    public static class PigSecondaryKeyGroupComparator extends WritableComparator {
        public PigSecondaryKeyGroupComparator() {
            super(NullableTuple.class, true);
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(WritableComparable a, WritableComparable b)
        {
            PigNullableWritable wa = (PigNullableWritable)a;
            PigNullableWritable wb = (PigNullableWritable)b;
            if ((wa.getIndex() & PigNullableWritable.mqFlag) != 0) { // this is a multi-query index
                if ((wa.getIndex() & PigNullableWritable.idxSpace) < (wb.getIndex() & PigNullableWritable.idxSpace)) return -1;
                else if ((wa.getIndex() & PigNullableWritable.idxSpace) > (wb.getIndex() & PigNullableWritable.idxSpace)) return 1;
                // If equal, we fall through
            }

            // wa and wb are guaranteed to be not null, POLocalRearrange will create a tuple anyway even if main key and secondary key
            // are both null; however, main key can be null, we need to check for that using the same logic we have in PigNullableWritable
            Object valuea = null;
            Object valueb = null;
            try {
                // Get the main key from compound key
                valuea = ((Tuple)wa.getValueAsPigType()).get(0);
                valueb = ((Tuple)wb.getValueAsPigType()).get(0);
            } catch (ExecException e) {
                throw new RuntimeException("Unable to access tuple field", e);
            }
            if (!wa.isNull() && !wb.isNull()) {

                int result = DataType.compare(valuea, valueb);

                // If any of the field inside tuple is null, then we do not merge keys
                // See PIG-927
                if (result == 0 && valuea instanceof Tuple && valueb instanceof Tuple)
                {
                    try {
                        for (int i=0;i<((Tuple)valuea).size();i++)
                            if (((Tuple)valueb).get(i)==null)
                                return (wa.getIndex()&PigNullableWritable.idxSpace) - (wb.getIndex()&PigNullableWritable.idxSpace);
                    } catch (ExecException e) {
                        throw new RuntimeException("Unable to access tuple field", e);
                    }
                }
                return result;
            } else if (valuea==null && valueb==null) {
                // If they're both null, compare the indicies
                if ((wa.getIndex() & PigNullableWritable.idxSpace) < (wb.getIndex() & PigNullableWritable.idxSpace)) return -1;
                else if ((wa.getIndex() & PigNullableWritable.idxSpace) > (wb.getIndex() & PigNullableWritable.idxSpace)) return 1;
                else return 0;
            }
            else if (valuea==null) return -1;
            else return 1;
        }
    }

    public static class PigWritableComparator extends WritableComparator {
        @SuppressWarnings("unchecked")
        protected PigWritableComparator(Class c) {
            super(c);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class PigBooleanWritableComparator extends PigWritableComparator {
        public PigBooleanWritableComparator() {
            super(NullableBooleanWritable.class);
        }
    }

    public static class PigIntWritableComparator extends PigWritableComparator {
        public PigIntWritableComparator() {
            super(NullableIntWritable.class);
        }
    }

    public static class PigLongWritableComparator extends PigWritableComparator {
        public PigLongWritableComparator() {
            super(NullableLongWritable.class);
        }
    }

    public static class PigFloatWritableComparator extends PigWritableComparator {
        public PigFloatWritableComparator() {
            super(NullableFloatWritable.class);
        }
    }

    public static class PigDoubleWritableComparator extends PigWritableComparator {
        public PigDoubleWritableComparator() {
            super(NullableDoubleWritable.class);
        }
    }

    public static class PigBigIntegerWritableComparator extends PigWritableComparator {
        public PigBigIntegerWritableComparator() {
            super(NullableBigIntegerWritable.class);
        }
    }

    public static class PigBigDecimalWritableComparator extends PigWritableComparator {
        public PigBigDecimalWritableComparator() {
            super(NullableBigDecimalWritable.class);
        }
    }

    public static class PigDateTimeWritableComparator extends PigWritableComparator {
        public PigDateTimeWritableComparator() {
            super(NullableDateTimeWritable.class);
        }
    }

    public static class PigCharArrayWritableComparator extends PigWritableComparator {
        public PigCharArrayWritableComparator() {
            super(NullableText.class);
        }
    }

    public static class PigDBAWritableComparator extends PigWritableComparator {
        public PigDBAWritableComparator() {
            super(NullableBytesWritable.class);
        }
    }

    public static class PigTupleWritableComparator extends PigWritableComparator {
        public PigTupleWritableComparator() {
            super(TupleFactory.getInstance().tupleClass());
        }
    }

    public static class PigBagWritableComparator extends PigWritableComparator {
        public PigBagWritableComparator() {
            super(BagFactory.getInstance().newDefaultBag().getClass());
        }
    }

    // XXX hadoop 20 new API integration: we need to explicitly set the Grouping
    // Comparator
    public static class PigGroupingBooleanWritableComparator extends WritableComparator {
        public PigGroupingBooleanWritableComparator() {
            super(NullableBooleanWritable.class, true);
        }
    }

    public static class PigGroupingIntWritableComparator extends WritableComparator {
        public PigGroupingIntWritableComparator() {
            super(NullableIntWritable.class, true);
        }
    }

    public static class PigGroupingLongWritableComparator extends WritableComparator {
        public PigGroupingLongWritableComparator() {
            super(NullableLongWritable.class, true);
        }
    }

    public static class PigGroupingFloatWritableComparator extends WritableComparator {
        public PigGroupingFloatWritableComparator() {
            super(NullableFloatWritable.class, true);
        }
    }

    public static class PigGroupingDoubleWritableComparator extends WritableComparator {
        public PigGroupingDoubleWritableComparator() {
            super(NullableDoubleWritable.class, true);
        }
    }

    public static class PigGroupingDateTimeWritableComparator extends WritableComparator {
        public PigGroupingDateTimeWritableComparator() {
            super(NullableDateTimeWritable.class, true);
        }
    }

    public static class PigGroupingCharArrayWritableComparator extends WritableComparator {
        public PigGroupingCharArrayWritableComparator() {
            super(NullableText.class, true);
        }
    }

    public static class PigGroupingDBAWritableComparator extends WritableComparator {
        public PigGroupingDBAWritableComparator() {
            super(NullableBytesWritable.class, true);
        }
    }

    public static class PigGroupingTupleWritableComparator extends WritableComparator {
        public PigGroupingTupleWritableComparator() {
            super(NullableTuple.class, true);
        }
    }

    public static class PigGroupingPartitionWritableComparator extends WritableComparator {
        public PigGroupingPartitionWritableComparator() {
            super(NullablePartitionWritable.class, true);
        }
    }

    public static class PigGroupingBagWritableComparator extends WritableComparator {
        public PigGroupingBagWritableComparator() {
            super(BagFactory.getInstance().newDefaultBag().getClass(), true);
        }
    }

    public static class PigGroupingBigIntegerWritableComparator extends WritableComparator {
        public PigGroupingBigIntegerWritableComparator() {
            super(NullableBigIntegerWritable.class, true);
        }
    }

    public static class PigGroupingBigDecimalWritableComparator extends WritableComparator {
        public PigGroupingBigDecimalWritableComparator() {
            super(NullableBigDecimalWritable.class, true);
        }
    }

    private void selectComparator(
            MapReduceOper mro,
            byte keyType,
            org.apache.hadoop.mapreduce.Job job) throws JobCreationException {
        // If this operator is involved in an order by, use the pig specific raw
        // comparators.  If it has a cogroup, we need to set the comparator class
        // to the raw comparator and the grouping comparator class to pig specific
        // raw comparators (which skip the index).  Otherwise use the hadoop provided
        // raw comparator.

        // An operator has an order by if global sort is set or if it's successor has
        // global sort set (because in that case it's the sampling job) or if
        // it's a limit after a sort.
        boolean hasOrderBy = false;
        if (mro.isGlobalSort() || mro.isLimitAfterSort() || mro.usingTypedComparator()) {
            hasOrderBy = true;
        } else {
            List<MapReduceOper> succs = plan.getSuccessors(mro);
            if (succs != null) {
                MapReduceOper succ = succs.get(0);
                if (succ.isGlobalSort()) hasOrderBy = true;
            }
        }
        if (hasOrderBy) {
            switch (keyType) {
            case DataType.BOOLEAN:
                job.setSortComparatorClass(PigBooleanRawComparator.class);
                break;

            case DataType.INTEGER:
                job.setSortComparatorClass(PigIntRawComparator.class);
                break;

            case DataType.LONG:
                job.setSortComparatorClass(PigLongRawComparator.class);
                break;

            case DataType.FLOAT:
                job.setSortComparatorClass(PigFloatRawComparator.class);
                break;

            case DataType.DOUBLE:
                job.setSortComparatorClass(PigDoubleRawComparator.class);
                break;

            case DataType.DATETIME:
                job.setSortComparatorClass(PigDateTimeRawComparator.class);
                break;

            case DataType.CHARARRAY:
                job.setSortComparatorClass(PigTextRawComparator.class);
                break;

            case DataType.BYTEARRAY:
                job.setSortComparatorClass(PigBytesRawComparator.class);
                break;

            case DataType.BIGINTEGER:
                job.setSortComparatorClass(PigBigIntegerRawComparator.class);
                break;

            case DataType.BIGDECIMAL:
                job.setSortComparatorClass(PigBigDecimalRawComparator.class);
                break;

            case DataType.MAP:
                int errCode = 1068;
                String msg = "Using Map as key not supported.";
                throw new JobCreationException(msg, errCode, PigException.INPUT);

            case DataType.TUPLE:
                job.setSortComparatorClass(PigTupleSortComparator.class);
                break;

            case DataType.BAG:
                errCode = 1068;
                msg = "Using Bag as key not supported.";
                throw new JobCreationException(msg, errCode, PigException.INPUT);

            default:
                break;
            }
            return;
        }

        switch (keyType) {
        case DataType.BOOLEAN:
            job.setSortComparatorClass(PigBooleanWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingBooleanWritableComparator.class);
            break;

        case DataType.INTEGER:
            job.setSortComparatorClass(PigIntWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingIntWritableComparator.class);
            break;

        case DataType.BIGINTEGER:
            job.setSortComparatorClass(PigBigIntegerWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingBigIntegerWritableComparator.class);
            break;

        case DataType.BIGDECIMAL:
            job.setSortComparatorClass(PigBigDecimalWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingBigDecimalWritableComparator.class);
            break;

        case DataType.LONG:
            job.setSortComparatorClass(PigLongWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingLongWritableComparator.class);
            break;

        case DataType.FLOAT:
            job.setSortComparatorClass(PigFloatWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingFloatWritableComparator.class);
            break;

        case DataType.DOUBLE:
            job.setSortComparatorClass(PigDoubleWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingDoubleWritableComparator.class);
            break;

        case DataType.DATETIME:
            job.setSortComparatorClass(PigDateTimeWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingDateTimeWritableComparator.class);
            break;

        case DataType.CHARARRAY:
            job.setSortComparatorClass(PigCharArrayWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingCharArrayWritableComparator.class);
            break;

        case DataType.BYTEARRAY:
            job.setSortComparatorClass(PigDBAWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingDBAWritableComparator.class);
            break;

        case DataType.MAP:
            int errCode = 1068;
            String msg = "Using Map as key not supported.";
            throw new JobCreationException(msg, errCode, PigException.INPUT);

        case DataType.TUPLE:
            job.setSortComparatorClass(PigTupleWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingTupleWritableComparator.class);
            break;

        case DataType.BAG:
            errCode = 1068;
            msg = "Using Bag as key not supported.";
            throw new JobCreationException(msg, errCode, PigException.INPUT);

        default:
            errCode = 2036;
            msg = "Unhandled key type " + DataType.findTypeName(keyType);
            throw new JobCreationException(msg, errCode, PigException.BUG);
        }
    }

    private void setupDistributedCacheForJoin(MapReduceOper mro,
            PigContext pigContext, Configuration conf) throws IOException {

        new JoinDistributedCacheVisitor(mro.mapPlan, pigContext, conf)
        .visit();

        new JoinDistributedCacheVisitor(mro.reducePlan, pigContext, conf)
        .visit();
    }

    private void setupDistributedCacheForUdfs(MapReduceOper mro,
            PigContext pigContext,
            Configuration conf) throws IOException {
        new UdfDistributedCacheVisitor(mro.mapPlan, pigContext, conf).visit();
        new UdfDistributedCacheVisitor(mro.reducePlan, pigContext, conf).visit();
    }

    private static void setupDistributedCache(PigContext pigContext,
            Configuration conf,
            Properties properties, String key,
            boolean shipToCluster)
                    throws IOException {
        // Set up the DistributedCache for this job
        String fileNames = properties.getProperty(key);

                if (fileNames != null) {
                    String[] paths = fileNames.split(",");
                    setupDistributedCache(pigContext, conf, paths, shipToCluster);
                }
    }

    private static void addToDistributedCache(URI uri, Configuration conf) {
        if (DISTRIBUTED_CACHE_ARCHIVE_MATCHER.reset(uri.toString()).find()) {
            DistributedCache.addCacheArchive(uri, conf);
        } else {
            DistributedCache.addCacheFile(uri, conf);
        }
    }

    private static void setupDistributedCache(PigContext pigContext,
            Configuration conf, String[] paths, boolean shipToCluster) throws IOException {
        // Turn on the symlink feature
        DistributedCache.createSymlink(conf);

        for (String path : paths) {
            path = path.trim();
            if (path.length() != 0) {
                Path src = new Path(path);

                // Ensure that 'src' is a valid URI
                URI srcURI = toURI(src);

                // Ship it to the cluster if necessary and add to the
                // DistributedCache
                if (shipToCluster) {
                    Path dst =
                            new Path(FileLocalizer.getTemporaryPath(pigContext).toString());
                    FileSystem fs = dst.getFileSystem(conf);
                    fs.copyFromLocalFile(src, dst);

                    // Construct the dst#srcName uri for DistributedCache
                    URI dstURI = null;
                    try {
                        dstURI = new URI(dst.toString() + "#" + src.getName());
                    } catch (URISyntaxException ue) {
                        byte errSrc = pigContext.getErrorSource();
                        int errCode = 0;
                        switch(errSrc) {
                        case PigException.REMOTE_ENVIRONMENT:
                            errCode = 6004;
                            break;
                        case PigException.USER_ENVIRONMENT:
                            errCode = 4004;
                            break;
                        default:
                            errCode = 2037;
                            break;
                        }
                        String msg = "Invalid ship specification. " +
                                "File doesn't exist: " + dst;
                        throw new ExecException(msg, errCode, errSrc);
                    }
                    addToDistributedCache(dstURI, conf);
                } else {
                    addToDistributedCache(srcURI, conf);
                }
            }
        }
    }

    private static String addSingleFileToDistributedCache(
            PigContext pigContext, Configuration conf, String filename,
            String prefix) throws IOException {

        if (!pigContext.inIllustrator && !FileLocalizer.fileExists(filename, pigContext)) {
            throw new IOException(
                    "Internal error: skew join partition file "
                            + filename + " does not exist");
        }

        String symlink = filename;

        // XXX Hadoop currently doesn't support distributed cache in local mode.
        // This line will be removed after the support is added by Hadoop team.
        if (!Utils.isLocal(pigContext, conf)) {
            symlink = prefix + "_"
                    + Integer.toString(System.identityHashCode(filename)) + "_"
                    + Long.toString(System.currentTimeMillis());
            filename = filename + "#" + symlink;
            setupDistributedCache(pigContext, conf, new String[] { filename },
                    false);
        }

        return symlink;
    }


    /**
     * Ensure that 'src' is a valid URI
     * @param src the source Path
     * @return a URI for this path
     * @throws ExecException
     */
    private static URI toURI(Path src) throws ExecException {
        String pathInString = src.toString();
        String fragment = null;
        if (pathInString.contains("#")) {
            fragment = pathInString.substring(pathInString.indexOf("#"));
            pathInString = pathInString.substring(0, pathInString.indexOf("#"));
        }

        // Encode the path
        URI uri = new Path(pathInString).toUri();
        String uriEncoded = uri.toString();
        if (fragment!=null) {
            uriEncoded = uriEncoded + fragment;
        }
        try {
            return new URI(uriEncoded);
        } catch (URISyntaxException ue) {
            int errCode = 6003;
            String msg = "Invalid cache specification. " +
                    "File doesn't exist: " + src;
            throw new ExecException(msg, errCode, PigException.USER_ENVIRONMENT);
        }
    }

    /**
     * if url is not in HDFS will copy the path to HDFS from local before adding to distributed cache
     * @param pigContext the pigContext
     * @param conf the job conf
     * @param url the url to be added to distributed cache
     * @return the path as seen on distributed cache
     * @throws IOException
     */
    @SuppressWarnings("deprecation")
    private static void putJarOnClassPathThroughDistributedCache(
            PigContext pigContext,
            Configuration conf,
            URL url) throws IOException {

        // Turn on the symlink feature
        DistributedCache.createSymlink(conf);

        // REGISTER always copies locally the jar file. see PigServer.registerJar()
        Path pathInHDFS = shipToHDFS(pigContext, conf, url);
        // and add to the DistributedCache
        DistributedCache.addFileToClassPath(pathInHDFS, conf);
        pigContext.addSkipJar(url.getPath());
    }

    private static Path getCacheStagingDir(Configuration conf) throws IOException {
        String pigTempDir = conf.get(PigConfiguration.PIG_USER_CACHE_LOCATION,
                conf.get(PigConfiguration.PIG_TEMP_DIR, "/tmp"));
        String currentUser = System.getProperty("user.name");
        Path stagingDir = new Path(pigTempDir + "/" + currentUser + "/", ".pigcache");
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(stagingDir);
        fs.setPermission(stagingDir, FileLocalizer.OWNER_ONLY_PERMS);
        return stagingDir;
    }

    private static Path getFromCache(PigContext pigContext,
            Configuration conf,
            URL url) throws IOException {
        InputStream is1 = null;
        InputStream is2 = null;
        OutputStream os = null;

        try {
            Path stagingDir = getCacheStagingDir(conf);
            String filename = FilenameUtils.getName(url.getPath());

            is1 = url.openStream();
            String checksum = DigestUtils.shaHex(is1);
            FileSystem fs = FileSystem.get(conf);
            Path cacheDir = new Path(stagingDir, checksum);
            Path cacheFile = new Path(cacheDir, filename);
            if (fs.exists(cacheFile)) {
                log.debug("Found " + url + " in jar cache at "+ cacheDir);
                long curTime = System.currentTimeMillis();
                fs.setTimes(cacheFile, -1, curTime);
                return cacheFile;
            }
            log.info("Url "+ url + " was not found in jarcache at "+ cacheDir);
            // attempt to copy to cache else return null
            fs.mkdirs(cacheDir, FileLocalizer.OWNER_ONLY_PERMS);
            is2 = url.openStream();
            os = FileSystem.create(fs, cacheFile, FileLocalizer.OWNER_ONLY_PERMS);
            IOUtils.copyBytes(is2, os, 4096, true);

            return cacheFile;

        } catch (IOException ioe) {
            log.info("Unable to retrieve jar from jar cache ", ioe);
            return null;
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(is1);
            org.apache.commons.io.IOUtils.closeQuietly(is2);
            // IOUtils should not close stream to HDFS quietly
            if (os != null) {
                os.close();
            }
        }
    }

    /**
     * copy the file to hdfs in a temporary path
     * @param pigContext the pig context
     * @param conf the job conf
     * @param url the url to ship to hdfs
     * @return the location where it was shipped
     * @throws IOException
     */
    private static Path shipToHDFS(
            PigContext pigContext,
            Configuration conf,
            URL url) throws IOException {

        boolean cacheEnabled =
                conf.getBoolean(PigConfiguration.PIG_USER_CACHE_ENABLED, false);
        if (cacheEnabled) {
            Path pathOnDfs = getFromCache(pigContext, conf, url);
            if(pathOnDfs != null) {
                return pathOnDfs;
            }
        }
        String suffix = FilenameUtils.getName(url.getPath());

        Path dst = new Path(FileLocalizer.getTemporaryPath(pigContext).toUri().getPath(), suffix);
        FileSystem fs = dst.getFileSystem(conf);
        OutputStream os = null;
        InputStream is = null;
        try {
            is = url.openStream();
            os = fs.create(dst);
            IOUtils.copyBytes(is, os, 4096, true);
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(is);
            // IOUtils should not close stream to HDFS quietly
            if (os != null) {
                os.close();
            }
        }
        return dst;
    }


    private static class JoinDistributedCacheVisitor extends PhyPlanVisitor {

        private PigContext pigContext = null;

        private Configuration conf = null;

        public JoinDistributedCacheVisitor(PhysicalPlan plan,
                PigContext pigContext, Configuration conf) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                    plan));
            this.pigContext = pigContext;
            this.conf = conf;
        }

        @Override
        public void visitFRJoin(POFRJoin join) throws VisitorException {

            // XXX Hadoop currently doesn't support distributed cache in local mode.
            // This line will be removed after the support is added
            if (Utils.isLocal(pigContext, conf)) return;

            // set up distributed cache for the replicated files
            FileSpec[] replFiles = join.getReplFiles();
            ArrayList<String> replicatedPath = new ArrayList<String>();

            FileSpec[] newReplFiles = new FileSpec[replFiles.length];

            // the first input is not replicated
            long sizeOfReplicatedInputs = 0;
            try {
                for (int i = 0; i < replFiles.length; i++) {
                    // ignore fragmented file
                    String symlink = "";
                    if (i != join.getFragment()) {
                        symlink = "pigrepl_" + join.getOperatorKey().toString() + "_"
                                + Integer.toString(System.identityHashCode(
                                        replFiles[i].getFileName()))
                                        + "_" + Long.toString(System.currentTimeMillis())
                                        + "_" + i;
                        replicatedPath.add(replFiles[i].getFileName() + "#"
                                + symlink);

                        Path path = new Path(replFiles[i].getFileName());
                        FileSystem fs = path.getFileSystem(conf);
                        sizeOfReplicatedInputs +=
                                MapRedUtil.getPathLength(fs, fs.getFileStatus(path));
                    }
                    newReplFiles[i] = new FileSpec(symlink,
                            (replFiles[i] == null ? null : replFiles[i].getFuncSpec()));
                }

                join.setReplFiles(newReplFiles);

                String maxSize = pigContext.getProperties().getProperty(
                        PigConfiguration.PIG_JOIN_REPLICATED_MAX_BYTES, "1000000000");
                if (sizeOfReplicatedInputs > Long.parseLong(maxSize)){
                    throw new VisitorException("Replicated input files size: "
                            + sizeOfReplicatedInputs + " exceeds " +
                            PigConfiguration.PIG_JOIN_REPLICATED_MAX_BYTES + ": " + maxSize);
                }

                setupDistributedCache(pigContext, conf, replicatedPath
                        .toArray(new String[0]), false);
            } catch (IOException e) {
                String msg = "Internal error. Distributed cache could not " +
                        "be set up for the replicated files";
                throw new VisitorException(msg, e);
            }
        }

        @Override
        public void visitMergeJoin(POMergeJoin join) throws VisitorException {

            // XXX Hadoop currently doesn't support distributed cache in local mode.
            // This line will be removed after the support is added
            if (Utils.isLocal(pigContext, conf)) return;

            String indexFile = join.getIndexFile();

            // merge join may not use an index file
            if (indexFile == null) return;

            try {
                String symlink = addSingleFileToDistributedCache(pigContext,
                        conf, indexFile, "indexfile_");
                join.setIndexFile(symlink);
            } catch (IOException e) {
                String msg = "Internal error. Distributed cache could not " +
                        "be set up for merge join index file";
                throw new VisitorException(msg, e);
            }
        }

        @Override
        public void visitMergeCoGroup(POMergeCogroup mergeCoGrp)
                throws VisitorException {

            // XXX Hadoop currently doesn't support distributed cache in local mode.
            // This line will be removed after the support is added
            if (Utils.isLocal(pigContext, conf)) return;

            String indexFile = mergeCoGrp.getIndexFileName();

            if (indexFile == null) throw new VisitorException("No index file");

            try {
                String symlink = addSingleFileToDistributedCache(pigContext,
                        conf, indexFile, "indexfile_mergecogrp_");
                mergeCoGrp.setIndexFileName(symlink);
            } catch (IOException e) {
                String msg = "Internal error. Distributed cache could not " +
                        "be set up for merge cogrp index file";
                throw new VisitorException(msg, e);
            }
        }
    }

    private static class UdfDistributedCacheVisitor extends PhyPlanVisitor {

        private PigContext pigContext = null;
        private Configuration conf = null;

        public UdfDistributedCacheVisitor(PhysicalPlan plan,
                PigContext pigContext,
                Configuration conf) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                    plan));
            this.pigContext = pigContext;
            this.conf = conf;
        }

        @Override
        public void visitUserFunc(POUserFunc func) throws VisitorException {

            // XXX Hadoop currently doesn't support distributed cache in local mode.
            // This line will be removed after the support is added
            if (Utils.isLocal(pigContext, conf)) return;

            // set up distributed cache for files indicated by the UDF
            String[] files = func.getCacheFiles();
            if (files == null) return;

            try {
                setupDistributedCache(pigContext, conf, files, false);
            } catch (IOException e) {
                String msg = "Internal error. Distributed cache could not " +
                        "be set up for the requested files";
                throw new VisitorException(msg, e);
            }
        }
    }

    private static class ParallelConstantVisitor extends PhyPlanVisitor {

        private int rp;

        private boolean replaced = false;

        public ParallelConstantVisitor(PhysicalPlan plan, int rp) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                    plan));
            this.rp = rp;
        }

        @Override
        public void visitConstant(ConstantExpression cnst) throws VisitorException {
            if (cnst.getRequestedParallelism() == -1) {
                Object obj = cnst.getValue();
                if (obj instanceof Integer) {
                    if (replaced) {
                        // sample job should have only one ConstantExpression
                        throw new VisitorException("Invalid reduce plan: more " +
                                "than one ConstantExpression found in sampling job");
                    }
                    cnst.setValue(rp);
                    cnst.setRequestedParallelism(rp);
                    replaced = true;
                }
            }
        }
    }

    public static void setOutputFormat(org.apache.hadoop.mapreduce.Job job) {
        // the OutputFormat we report to Hadoop is always PigOutputFormat which
        // can be wrapped with LazyOutputFormat provided if it is supported by
        // the Hadoop version and PigConfiguration.PIG_OUTPUT_LAZY is set
        if ("true".equalsIgnoreCase(job.getConfiguration().get(PigConfiguration.PIG_OUTPUT_LAZY))) {
            try {
                Class<?> clazz = PigContext
                        .resolveClassName("org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat");
                Method method = clazz.getMethod("setOutputFormatClass",
                        org.apache.hadoop.mapreduce.Job.class, Class.class);
                method.invoke(null, job, PigOutputFormat.class);
            } catch (Exception e) {
                job.setOutputFormatClass(PigOutputFormat.class);
                log.warn(PigConfiguration.PIG_OUTPUT_LAZY
                        + " is set but LazyOutputFormat couldn't be loaded. Default PigOutputFormat will be used");
            }
        } else {
            job.setOutputFormatClass(PigOutputFormat.class);
        }
    }

}
