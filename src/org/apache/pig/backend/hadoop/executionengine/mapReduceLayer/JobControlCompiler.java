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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.ComparisonFunc;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.SecondaryKeyPartitioner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.SkewedPartitioner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.WeightedRangePartitioner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullablePartitionWritable;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UDFContext;


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
public class JobControlCompiler{
    MROperPlan plan;
    Configuration conf;
    PigContext pigContext;
    
    private final Log log = LogFactory.getLog(getClass());
    
    public static final String LOG_DIR = "_logs";

    public static final String END_OF_INP_IN_MAP = "pig.invoke.close.in.map";
    
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
    public static final String PIG_STORE_FUNC = "pig.storeFunc";
    
    // A mapping of job to pair of store locations and tmp locations for that job
    private Map<Job, Pair<List<POStore>, Path>> jobStoreMap;
    
    private Map<Job, MapReduceOper> jobMroMap;

    public JobControlCompiler(PigContext pigContext, Configuration conf) throws IOException {
        this.pigContext = pigContext;
        this.conf = conf;
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

    private Path makeTmpPath() throws IOException {
        Path tmpPath = null;
        for (int tries = 0;;) {
            try {
                tmpPath = 
                    new Path(FileLocalizer
                             .getTemporaryPath(null, pigContext).toString());
                FileSystem fs = tmpPath.getFileSystem(conf);
                tmpPath = tmpPath.makeQualified(fs);
                fs.mkdirs(tmpPath);
                break;
            } catch (IOException ioe) {
                if (++tries==100) {
                    throw ioe;
                }
            }
        }
        return tmpPath;
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
        this.plan = plan;

        if (plan.size() == 0) {
            return null;
        }

        JobControl jobCtrl = new JobControl(grpName);

        try {
            List<MapReduceOper> roots = new LinkedList<MapReduceOper>();
            roots.addAll(plan.getRoots());
            for (MapReduceOper mro: roots) {
                Job job = getJob(mro, conf, pigContext);
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
                plan.remove(mro);
            }
        }
        jobMroMap.clear();
        int sizeAfter = plan.size();
        return sizeBefore-sizeAfter;
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
     * @param conf - the Configuration object from which JobConf is built
     * @param pigContext - The PigContext passed on from execution engine
     * @return Job corresponding to mro
     * @throws JobCreationException
     */
    @SuppressWarnings({ "unchecked", "deprecation" })
    private Job getJob(MapReduceOper mro, Configuration config, PigContext pigContext) throws JobCreationException{
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
        ArrayList<POStore> storeLocations = new ArrayList<POStore>();
        Path tmpLocation = null;
        
        //Set the User Name for this job. This will be
        //used as the working directory
        String user = System.getProperty("user.name");        
        conf.set("user.name", (user != null ? user : "Pigster"));
        if (pigContext.defaultParallel > 0)
            conf.set("mapred.reduce.tasks", ""+pigContext.defaultParallel);
 
        conf.set("mapred.mapper.new-api", "true");
        conf.set("mapred.reducer.new-api", "true");
        
        String buffPercent = conf.get("mapred.job.reduce.markreset.buffer.percent");
        if (buffPercent == null || Double.parseDouble(buffPercent) <= 0) {
            log.info("mapred.job.reduce.markreset.buffer.percent is not set, set to default 0.3");
            conf.set("mapred.job.reduce.markreset.buffer.percent", "0.3");
        }else{
            log.info("mapred.job.reduce.markreset.buffer.percent is set to " + conf.get("mapred.job.reduce.markreset.buffer.percent"));
        }        
                
        try{        
            //Process the POLoads
            List<POLoad> lds = PlanHelper.getLoads(mro.mapPlan);
            
            if(lds!=null && lds.size()>0){
                for (POLoad ld : lds) {
                    
                    //Store the inp filespecs
                    inp.add(ld.getLFile());
                    
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
                    //Remove the POLoad from the plan
                    mro.mapPlan.remove(ld);
                }
            }

            //Create the jar of all functions reuired
            File submitJarFile = File.createTempFile("Job", ".jar");
            // ensure the job jar is deleted on exit
            submitJarFile.deleteOnExit();
            FileOutputStream fos = new FileOutputStream(submitJarFile);
            JarManager.createJar(fos, mro.UDFs, pigContext);
            
            //Start setting the JobConf properties
            conf.set("mapred.jar", submitJarFile.getPath());
            conf.set("pig.inputs", ObjectSerializer.serialize(inp));
            conf.set("pig.inpTargets", ObjectSerializer.serialize(inpTargets));
            conf.set("pig.inpSignatures", ObjectSerializer.serialize(inpSignatureLists));
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

            // Setup the DistributedCache for this job
            setupDistributedCache(pigContext, nwJob.getConfiguration(), pigContext.getProperties(), 
                                  "pig.streaming.ship.files", true);
            setupDistributedCache(pigContext, nwJob.getConfiguration(), pigContext.getProperties(), 
                                  "pig.streaming.cache.files", false);

            nwJob.setInputFormatClass(PigInputFormat.class);
            
            //Process POStore and remove it from the plan
            LinkedList<POStore> mapStores = PlanHelper.getStores(mro.mapPlan);
            LinkedList<POStore> reduceStores = PlanHelper.getStores(mro.reducePlan);
            
            conf.set(PIG_MAP_STORES, ObjectSerializer.serialize(mapStores));
            conf.set(PIG_REDUCE_STORES, ObjectSerializer.serialize(reduceStores));
            
            for (POStore st: mapStores) {
                storeLocations.add(st);
                StoreFunc sFunc = st.getStoreFunc();
                if (st.getSchema()!=null)
                    sFunc.checkSchema(new ResourceSchema(st.getSchema(), st.getSortInfo()));
                sFunc.setStoreLocation(st.getSFile().getFileName(), nwJob);
            }

            for (POStore st: reduceStores) {
                storeLocations.add(st);
                StoreFunc sFunc = st.getStoreFunc();
                if (st.getSchema()!=null)
                    sFunc.checkSchema(new ResourceSchema(st.getSchema(), st.getSortInfo()));
                sFunc.setStoreLocation(st.getSFile().getFileName(), nwJob);
            }

            // the OutputFormat we report to Hadoop is always PigOutputFormat
            nwJob.setOutputFormatClass(PigOutputFormat.class);
            
            if (mapStores.size() + reduceStores.size() == 1) { // single store case
                log.info("Setting up single store job");
                
                POStore st;
                if (reduceStores.isEmpty()) {
                    st = mapStores.remove(0);
                    mro.mapPlan.remove(st);
                }
                else {
                    st = reduceStores.remove(0);
                    mro.reducePlan.remove(st);
                }

                // set out filespecs
                String outputPath = st.getSFile().getFileName();
                FuncSpec outputFuncSpec = st.getSFile().getFuncSpec();
                
                // serialize the store func spec using ObjectSerializer
                // ObjectSerializer.serialize() uses default java serialization
                // and then further encodes the output so that control characters
                // get encoded as regular characters. Otherwise any control characters
                // in the store funcspec would break the job.xml which is created by
                // hadoop from the jobconf.
                conf.set(PIG_STORE_FUNC, ObjectSerializer.serialize(outputFuncSpec.toString()));
                conf.set("pig.streaming.log.dir", 
                            new Path(outputPath, LOG_DIR).toString());
                conf.set("pig.streaming.task.output.dir", outputPath);
            } 
           else { // multi store case
                log.info("Setting up multi store job");
                String tmpLocationStr =  FileLocalizer
                .getTemporaryPath(null, pigContext).toString();
                tmpLocation = new Path(tmpLocationStr);

                nwJob.setOutputFormatClass(PigOutputFormat.class);
 
                conf.set("pig.streaming.log.dir", 
                            new Path(tmpLocation, LOG_DIR).toString());
                conf.set("pig.streaming.task.output.dir", tmpLocation.toString());
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
            setupDistributedCacheForFRJoin(mro, pigContext, conf);

            POPackage pack = null;
            if(mro.reducePlan.isEmpty()){
                //MapOnly Job
                nwJob.setMapperClass(PigMapOnly.Map.class);
                nwJob.setNumReduceTasks(0);
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
                mro.reducePlan.remove(pack);
                nwJob.setMapperClass(PigMapReduce.Map.class);
                nwJob.setReducerClass(PigMapReduce.Reduce.class);
                if (mro.requestedParallelism>0)
                    nwJob.setNumReduceTasks(mro.requestedParallelism);

                conf.set("pig.mapPlan", ObjectSerializer.serialize(mro.mapPlan));
                if(mro.isEndOfAllInputSetInMap()) {
                    // this is used in Map.close() to decide whether the
                    // pipeline needs to be rerun one more time in the close()
                    // The pipeline is rerun only if there was a stream or merge-join.
                    conf.set(END_OF_INP_IN_MAP, "true");
                }
                conf.set("pig.reducePlan", ObjectSerializer.serialize(mro.reducePlan));
                if(mro.isEndOfAllInputSetInReduce()) {
                    // this is used in Map.close() to decide whether the
                    // pipeline needs to be rerun one more time in the close()
                    // The pipeline is rerun only if there was a stream
                    conf.set("pig.stream.in.reduce", "true");
                }
                conf.set("pig.reduce.package", ObjectSerializer.serialize(pack));
                conf.set("pig.reduce.key.type", Byte.toString(pack.getKeyType())); 
                
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
                    Class<? extends WritableComparable> keyClass = HDataType.getWritableComparableTypes(pack.getKeyType()).getClass();
                    nwJob.setOutputKeyClass(keyClass);
                    selectComparator(mro, pack.getKeyType(), nwJob);
                }
                nwJob.setOutputValueClass(NullableTuple.class);
            }
        
            if(mro.isGlobalSort() || mro.isLimitAfterSort()){
                // Only set the quantiles file and sort partitioner if we're a
                // global sort, not for limit after sort.
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
      
            // Serialize the UDF specific context info.
            UDFContext.getUDFContext().serialize(conf);
            Job cjob = new Job(new JobConf(nwJob.getConfiguration()), new ArrayList());
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
    
    public static class PigSecondaryKeyGroupComparator extends WritableComparator {
        @SuppressWarnings("unchecked")
        public PigSecondaryKeyGroupComparator() {
//            super(TupleFactory.getInstance().tupleClass(), true);
            super(NullableTuple.class, true);
        }

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
        if (mro.isGlobalSort() || mro.isLimitAfterSort()) {
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

            case DataType.CHARARRAY:
                job.setSortComparatorClass(PigTextRawComparator.class);
                break;

            case DataType.BYTEARRAY:
                job.setSortComparatorClass(PigBytesRawComparator.class);
                break;

            case DataType.MAP:
                int errCode = 1068;
                String msg = "Using Map as key not supported.";
                throw new JobCreationException(msg, errCode, PigException.INPUT);

            case DataType.TUPLE:
                job.setSortComparatorClass(PigTupleRawComparator.class);
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
        case DataType.INTEGER:
            job.setSortComparatorClass(PigIntWritableComparator.class);
            job.setGroupingComparatorClass(PigGroupingIntWritableComparator.class);
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

    private void setupDistributedCacheForFRJoin(MapReduceOper mro,
            PigContext pigContext, Configuration conf) throws IOException {       
                    
        new FRJoinDistributedCacheVisitor(mro.mapPlan, pigContext, conf)
                .visit();
             
        new FRJoinDistributedCacheVisitor(mro.reducePlan, pigContext, conf)
                .visit();
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
        
    private static void setupDistributedCache(PigContext pigContext,
            Configuration conf, String[] paths, boolean shipToCluster) throws IOException {
        // Turn on the symlink feature
        DistributedCache.createSymlink(conf);
            
        for (String path : paths) {
            path = path.trim();
            if (path.length() != 0) {
                Path src = new Path(path);
                
                // Ensure that 'src' is a valid URI
                URI srcURI = null;
                try {
                    srcURI = new URI(src.toString());
                } catch (URISyntaxException ue) {
                    int errCode = 6003;
                    String msg = "Invalid cache specification. " +
                    "File doesn't exist: " + src;
                    throw new ExecException(msg, errCode, PigException.USER_ENVIRONMENT);
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
                    DistributedCache.addCacheFile(dstURI, conf);
                } else {
                    DistributedCache.addCacheFile(srcURI, conf);
                }
            }
        }        
    }
    
    private static String addSingleFileToDistributedCache(
            PigContext pigContext, Configuration conf, String filename,
            String prefix) throws IOException {

        if (!FileLocalizer.fileExists(filename, pigContext)) {
            throw new IOException(
                    "Internal error: skew join partition file "
                    + filename + " does not exist");
        }
                     
        String symlink = filename;
                     
        // XXX Hadoop currently doesn't support distributed cache in local mode.
        // This line will be removed after the support is added by Hadoop team.
        if (pigContext.getExecType() != ExecType.LOCAL) {
            symlink = prefix + "_" 
                    + Integer.toString(System.identityHashCode(filename)) + "_"
                    + Long.toString(System.currentTimeMillis());
            filename = filename + "#" + symlink;
            setupDistributedCache(pigContext, conf, new String[] { filename },
                    false);  
        }
         
        return symlink;
    }
    
    private static class FRJoinDistributedCacheVisitor extends PhyPlanVisitor {
                 
        private PigContext pigContext = null;
                
         private Configuration conf = null;
         
         public FRJoinDistributedCacheVisitor(PhysicalPlan plan, 
                 PigContext pigContext, Configuration conf) {
             super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                     plan));
             this.pigContext = pigContext;
             this.conf = conf;
         }
         
         public void visitFRJoin(POFRJoin join) throws VisitorException {
             
             // XXX Hadoop currently doesn't support distributed cache in local mode.
             // This line will be removed after the support is added
             if (pigContext.getExecType() == ExecType.LOCAL) return;
             
             // set up distributed cache for the replicated files
             FileSpec[] replFiles = join.getReplFiles();
             ArrayList<String> replicatedPath = new ArrayList<String>();
             
             FileSpec[] newReplFiles = new FileSpec[replFiles.length];
             
             // the first input is not replicated
             for (int i = 0; i < replFiles.length; i++) {
                 // ignore fragmented file
                 String symlink = "";
                 if (i != join.getFragment()) {
                     symlink = "pigrepl_" + join.getOperatorKey().toString() + "_"
                         + Integer.toString(System.identityHashCode(replFiles[i].getFileName()))
                         + "_" + Long.toString(System.currentTimeMillis()) 
                         + "_" + i;
                     replicatedPath.add(replFiles[i].getFileName() + "#"
                             + symlink);
                 }
                 newReplFiles[i] = new FileSpec(symlink, 
                         (replFiles[i] == null ? null : replFiles[i].getFuncSpec()));               
             }
             
             join.setReplFiles(newReplFiles);
             
             try {
                 setupDistributedCache(pigContext, conf, replicatedPath
                         .toArray(new String[0]), false);
             } catch (IOException e) {
                 String msg = "Internal error. Distributed cache could not " +
                               "be set up for the replicated files";
                 throw new VisitorException(msg, e);
             }
         }
     }
    
}
